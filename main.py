import logging
import uuid
from itertools import islice
from typing import Iterator, List, Dict, Optional

from sqlalchemy import select

from amocrm import COMPANIES, CONTACTS, Customer as AmoCustomer, Lead as AmoLead, amocrm_get
from models import db, Customer, Lead
from repository import upsert_customers, upsert_leads, load_customers_data, load_leads_data, merge_customers, cleanup

logger = logging.getLogger(__name__)
BATCH_SIZE = 1000


def iter_customer_batches(batch_size: int = BATCH_SIZE) -> Iterator[List[AmoCustomer]]:
    """
    Читает всех покупателей из CRM и отдает пакетами по BATCH_SIZE
    """
    customers = amocrm_get(AmoCustomer, _with=f"{COMPANIES},{CONTACTS}")
    try:
        while True:
            batch = list(islice(customers, batch_size))
            if not batch:
                return
            yield batch
    finally:
        customers.close()


def iter_lead_batches(batch_size: int = BATCH_SIZE) -> Iterator[List[AmoLead]]:
    """
    Аналогично читает все сделки из CRM и отдает пакетами по BATCH_SIZE
    """
    leads = amocrm_get(AmoLead, _with=f"{CONTACTS}")
    try:
        while True:
            batch = list(islice(leads, batch_size))
            if not batch:
                return
            yield batch
    finally:
        leads.close()


def sync_customers(batch: List[AmoCustomer], run_id, is_full: bool = True) -> Dict[int, int]:
    """
    Записывает пакет покупателей: 1 UPSERT + 1 COMMIT на пакет
    Возвращает словарь {crm_customer_id: id}
    """
    rows = []
    for customer in batch:
        if is_full is False and customer.status_id == 111349:
            continue
        login = customer.get_field('Логин')
        if not login:
            continue
        rows.append({
            'crm_customer_id': customer.id,
            'login': login,
            'name': customer.name,
            'jsonb_data': customer.to_json(),
        })

    if not rows:
        return {}
    upsert_customers(rows, run_id)
    db.session.commit()

    # id покупателей, которых записали в этом пакете
    crm_ids = []
    for row in rows:
        crm_ids.append(row['crm_customer_id'])

    query = select(Customer.id, Customer.crm_customer_id).where(Customer.crm_customer_id.in_(crm_ids))
    # сопоставление crm_customer_id: id этого покупателя из БД
    saved = {}
    for customer_id, crm_customer_id in db.session.execute(query):
        saved[crm_customer_id] = customer_id

    return saved


def sync_leads(batch: List[AmoLead], run_id, customers_by_login: Dict[str, Optional[int]],
               leads_by_crm_id: Dict[int, tuple], is_full: bool = True) -> Dict[int, int]:
    """
    Записывает пакет сделок: 1 COMMIT на пачку
    Если есть связь покупатель-сделка, то она сохраняется, иначе поиск по логину среди покупателей
    Возвращает {crm_lead_id: id}
    """
    rows = []
    for lead in batch:
        if is_full is False and lead.status_id == 143:
            continue
        login = lead.get_field('Логин')
        existing = leads_by_crm_id.get(lead.id)
        customer_id = None
        if existing is not None:
            customer_id = existing[1]
        by_login_customer_id = None
        if login:
            by_login_customer_id = customers_by_login.get(login)
        if customer_id is not None and by_login_customer_id is not None and customer_id != by_login_customer_id:
            if merge_customers(customer_id, by_login_customer_id):
                customers_by_login[login] = customer_id
                for crm_lead_id, cached in list(leads_by_crm_id.items()):
                    if cached[1] == by_login_customer_id:
                        leads_by_crm_id[crm_lead_id] = (cached[0], customer_id)
        if customer_id is None:
            customer_id = by_login_customer_id

        rows.append({
            'crm_lead_id': lead.id,
            'login': login,
            'name': lead.name,
            'customer_id': customer_id,
            'jsonb_data': lead.to_json(),
        })

    if not rows:
        return {}
    upsert_leads(rows, run_id)
    db.session.commit()

    crm_ids = []
    for row in rows:
        crm_ids.append(row['crm_lead_id'])

    query = select(Lead.id, Lead.crm_lead_id).where(Lead.crm_lead_id.in_(crm_ids))
    saved = {}
    for lead_id, crm_lead_id in db.session.execute(query):
        saved[crm_lead_id] = lead_id

    return saved


def collect_leads_customers(is_full: bool = True) -> bool:
    """
    completed=True выставляется только после полного прохода без исключений, чистка запускается только по нему
    """
    run_id = uuid.uuid4()
    completed = False

    contact_ids = set()
    company_ids = set()

    customers_by_crm_id, customers_by_login = load_customers_data()
    leads_by_crm_id, _ = load_leads_data()
    logger.info(f"Started sync {run_id=} {is_full=} customers={len(customers_by_crm_id)} leads={len(leads_by_crm_id)}")

    try:
        for batch in iter_customer_batches():
            saved = sync_customers(batch, run_id, is_full)

            for customer in batch:
                customer_id = saved.get(customer.id)

                if customer_id is None:
                    continue

                customers_by_crm_id[customer.id] = customer_id
                for contact in customer.contacts:
                    if contact.get('id') is not None:
                        contact_ids.add(contact.get('id'))
                for company in customer.companies:
                    if company.get('id') is not None:
                        company_ids.add(company.get('id'))
                login = customer.get_field('Логин')

                if not login:
                    continue
                if login in customers_by_login and customers_by_login[login] != customer_id:
                    customers_by_login[login] = None
                else:
                    customers_by_login[login] = customer_id
        logger.info(f"Collected customers {run_id=}")

        for batch in iter_lead_batches():
            saved = sync_leads(batch, run_id, customers_by_login, leads_by_crm_id, is_full)

            for lead in batch:
                lead_id = saved.get(lead.id)

                if lead_id is None:
                    continue

                existing = leads_by_crm_id.get(lead.id)
                customer_id = None

                if existing is not None:
                    customer_id = existing[1]
                if customer_id is None:
                    customer_id = customers_by_login.get(lead.get_field('Логин'))

                leads_by_crm_id[lead.id] = (lead_id, customer_id)
                for contact in lead.contacts:
                    if contact.get('id') is not None:
                        contact_ids.add(contact.get('id'))
                for company in lead.companies:
                    if company.get('id') is not None:
                        company_ids.add(company.get('id'))

        logger.info(f"Collected leads {run_id=}")
        completed = True

    except Exception as e:
        logger.exception(f"Sync failed {run_id=}: {e}")

    if completed and is_full:
        result = cleanup(run_id, contact_ids, company_ids)
        db.session.commit()
        logger.info(f"Cleanup {run_id=} {result=}")

    return completed
