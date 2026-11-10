import logging
from itertools import islice
from typing import Iterator, List, Dict

from sqlalchemy import delete, select
from sqlalchemy.orm.attributes import flag_modified

from abstract_customer import get_abccustomer_by_lead_login, create_or_update_customer, \
    get_abccustomer_by_amocustomer_login, dict_compare_merge, create_or_update_lead
from amocrm import COMPANIES, CONTACTS, Customer as AmoCustomer, Lead as AmoLead, amocrm_get
from models import db, AbstractCustomer

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


def collect_leads_customers(is_full=True):
    """
     Берем всех покупателей, если его нет в бд - делаем новую запись AbstractCutomer, в него кладем.
     Есть есть - находим его запись по id и перезаписываем

     Вот так у нас коллектятся все кастомеры

     Далее берутся все лиды. Ищутся их записи в бд по id, если не находится и есть логин
     - находим его запись по кастомеру с тем же логином, кладем к нему.
     Если нет логина/нет кастомера - пропускаем. Если нашелся - перезаписываем соответственно
         abstract_id_customer : {
        customers: [...],
        leads: [...],
        contacts: [...],
        companies: [...]
    }
    """
    session = db.session
    customers: Iterator[AmoCustomer] = amocrm_get(AmoCustomer, _with=f"{COMPANIES},{CONTACTS}")
    logger.info(f"Collected customers {is_full=}")

    ids_map = {}
    for customer in customers:
        if is_full is False and customer.status_id == 111349:
            continue
        customer_login = customer.get_field('Логин')
        if not customer_login:
            continue
        abstract_customer = session.query(AbstractCustomer).filter(
            AbstractCustomer.amo_customers.op('@>')([{'general': {'id': customer.id}}])).first()

        if not abstract_customer:
            abstract_customer = get_abccustomer_by_lead_login(customer_login)  # почти всегда логины совпадают
        abstract_customer = create_or_update_customer(abstract_customer, customer)
        if is_full is True:
            if abstract_customer.id in ids_map:
                ids_map[abstract_customer.id]["customers"].append(customer.id)
                ids_map[abstract_customer.id]["contacts"].extend([c["id"] for c in customer.contacts])
                ids_map[abstract_customer.id]["companies"].extend([c["id"] for c in customer.companies])
            else:
                ids_map[abstract_customer.id] = {
                    "customers": [customer.id],
                    "contacts": [c["id"] for c in customer.contacts],
                    "companies": [c["id"] for c in customer.companies],
                    "leads": [],
                }
        db.session.commit()

    customers.close()

    leads: Iterator[AmoLead] = amocrm_get(AmoLead, _with=f"{CONTACTS}")
    logger.info(f"Collected leads {is_full=}")
    for lead in leads:
        if not is_full and lead.status_id == 143:
            continue
        try:
            abc_customer = lead_action(lead)
            if is_full:
                if abc_customer.id in ids_map:
                    ids_map[abc_customer.id]["leads"].append(lead.id)
                    ids_map[abc_customer.id]["contacts"].extend([c["id"] for c in lead.contacts])
                else:
                    ids_map[abc_customer.id] = {
                        "customers": [],
                        "contacts": [c["id"] for c in lead.contacts],
                        "companies": [],
                        "leads": [lead.id],
                    }
        except Exception as e:
            logger.exception(f'{lead} {e}')
    leads.close()

    if is_full is False:
        return
    try:
        logger.info("Чистка покупателей общая")
        stmt = delete(AbstractCustomer).where(
            AbstractCustomer.id.not_in(ids_map.keys())
        )
        db.session.execute(stmt)
        db.session.commit()
        logger.info("Чистка покупателей на уровне сущностей")
        for abc_customer in db.session.scalars(select(AbstractCustomer)).all():
            filter_abc_customer(abc_customer=abc_customer, ids_map=ids_map)
    except Exception as e:
        logger.exception(f"Чистка: {e}")


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
