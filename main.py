import logging
import uuid
from itertools import islice
from typing import Iterator, List, Dict, Optional

from sqlalchemy import delete, select
from sqlalchemy.orm.attributes import flag_modified

from abstract_customer import get_abccustomer_by_lead_login, create_or_update_customer, \
    get_abccustomer_by_amocustomer_login, dict_compare_merge, create_or_update_lead
from amocrm import COMPANIES, CONTACTS, Customer as AmoCustomer, Lead as AmoLead, amocrm_get
from models import db, AbstractCustomer, Customer, Lead
from repository import upsert_customers, upsert_leads, load_customers_data, load_leads_data, merge_customers

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


def lead_action(lead: AmoLead):
    abstract_customer = db.session.query(AbstractCustomer).filter(
        AbstractCustomer.amo_leads.op('@>')([{'general': {'id': lead.id}}])).first()
    lead_login = lead.get_field('Логин')
    if lead_login:
        abs_customer_login = get_abccustomer_by_amocustomer_login(lead_login)
        if abstract_customer is None:
            abstract_customer = abs_customer_login
        if abs_customer_login is not None:
            if abs_customer_login.id != abstract_customer.id:
                abstract_customer.amo_customers = dict_compare_merge(abstract_customer.amo_customers or [],
                                                                     abs_customer_login.amo_customers or [])
                abstract_customer.amo_leads = dict_compare_merge(abstract_customer.amo_leads or [],
                                                                 abs_customer_login.amo_leads or [])
                abstract_customer.amo_contacts = dict_compare_merge(abstract_customer.amo_contacts or [],
                                                                    abs_customer_login.amo_contacts or [])
                abstract_customer.amo_companies = dict_compare_merge(abstract_customer.amo_companies or [],
                                                                     abs_customer_login.amo_companies or [])
                flag_modified(abstract_customer, "amo_customers")
                flag_modified(abstract_customer, "amo_leads")
                flag_modified(abstract_customer, "amo_contacts")
                flag_modified(abstract_customer, "amo_companies")
                db.session.commit()
                db.session.flush()
                db.session.delete(abs_customer_login)
                db.session.commit()
                return

    return create_or_update_lead(abstract_customer, lead)


def collect_leads_customers(is_full: bool = True) -> bool:
    """
    completed=True выставляется только после полного прохода без исключений, чистка запускается только по нему
    """
    run_id = uuid.uuid4()
    completed = False

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
                login = customer.get_field('Логин')

                if not login:
                    continue
                if login in customers_by_login and customers_by_login[login] != customer_id:
                    customers_by_login[login] = None
                else:
                    customers_by_login[login] = customer_id

        # logger.info(f"Collected customers {run_id=} known={len(customers_by_crm_id)}")

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

        # logger.info(f"Collected leads {run_id=} known={len(leads_by_crm_id)}")
        completed = True

    except Exception as e:
        logger.exception(f"Sync failed {run_id=}: {e}")

    if completed and is_full:
        logger.info(f"Sync completed, cleanup pending {run_id=}")

    return completed


def filter_merge_abstract_customer(abc_customer: AbstractCustomer, attr: str,
                                   check_ids: List[int]):
    entity = abc_customer.__getattribute__(attr)
    for l in entity:
        entity = [c for c in entity if
                  'удалить' not in l['general'].get('name').lower() and l['general'].get('id') in check_ids]
        setattr(abc_customer, attr, entity)
        flag_modified(abc_customer, attr)
        db.session.commit()


def filter_abc_customer(abc_customer: AbstractCustomer, ids_map: Dict):
    if abc_customer.amo_leads:
        try:
            filter_merge_abstract_customer(abc_customer=abc_customer,
                                           attr="amo_leads",
                                           check_ids=ids_map[abc_customer.id]["leads"])
        except Exception as e:
            logger.exception(e)
    if abc_customer.amo_customers:
        try:
            filter_merge_abstract_customer(abc_customer=abc_customer,
                                           attr="amo_customers",
                                           check_ids=ids_map[abc_customer.id]["customers"])
        except Exception as e:
            logger.exception(e)
    if abc_customer.amo_contacts:
        try:
            filter_merge_abstract_customer(abc_customer=abc_customer,
                                           attr="amo_contacts",
                                           check_ids=ids_map[abc_customer.id]["contacts"])
        except Exception as e:
            logger.exception(e)
    if abc_customer.amo_companies:
        try:
            filter_merge_abstract_customer(abc_customer=abc_customer,
                                           attr="amo_companies",
                                           check_ids=ids_map[abc_customer.id]["companies"])
        except Exception as e:
            logger.exception(e)
    if not abc_customer.amo_leads and not abc_customer.amo_customers:
        db.session.flush()
        db.session.delete(abc_customer)
    db.session.commit()
