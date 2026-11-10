import json
import logging
from collections import defaultdict
from typing import Optional, Dict

from sqlalchemy import text

from amocrm import Customer, Lead
from models import AbstractCustomer, db, AbcCustomerEntities

logger = logging.getLogger(__name__)


def update_jsonb_field(entity_id: int,
                       attribute: AbcCustomerEntities,
                       index: int, new_value: Dict,
                       table_name='abstract_customer'):
    """Обновление jsonb поля в указанной таблице.
    :param table_name: Название таблицы (должно быть прописано).
    :param entity_id: ид абстрактного покупателя.
    :param attribute: Поле таблицы, которое надо обновить в формате Enum.
    :param index: Индекс изменения - цифра если массив, массив строк если словарь. Цифра должна быть точной.
    :param new_value: Новые данные для изменения
    """
    if table_name != 'abstract_customer':
        raise Exception("Неизвестная таблица")
    query = text(f"""
        UPDATE {table_name}
        SET {attribute.value} = jsonb_set({attribute.value}, :path, :value)
        WHERE id = :entity_id
    """)
    db.session.execute(query, {'path': f'{{{index}}}', 'value': json.dumps(new_value), 'entity_id': entity_id})


def dict_compare_merge(l_dict1, l_dict2):
    d = defaultdict(dict)
    for i in l_dict1 + l_dict2:
        d[i['general']['id']].update(i)

    return list(d.values())


def get_customer_json(customer: Customer) -> dict:
    return {
        'general': {
            'id': customer.id,
            'login': customer.custom_fields_values.get('Логин', {}).get('value'),
            'billing_type': customer.custom_fields_values.get('Вид оплаты', {}).get('value'),
            'is_robot': bool(customer.custom_fields_values.get('Робот')),
            'is_nds': bool(customer.custom_fields_values.get('Оплата с НДС')),
            'system': customer.custom_fields_values.get('Система', {}).get('value'),
            'host': customer.custom_fields_values.get('Хост', {}).get('value'),
            'name': customer.name,
        },
        'api_value': customer.to_json()
    }


def get_lead_json(lead: Lead) -> dict:
    return {
        'general': {
            'id': lead.id,
            'login': lead.custom_fields_values.get('Логин', {}).get('value'),
            'name': lead.name,
        },
        'api_value': lead.to_json()
    }


def get_abccustomer_by_lead_login(lead_login: str) -> Optional[AbstractCustomer]:
    customers = db.session.query(AbstractCustomer).filter(
        AbstractCustomer.amo_leads.op('@>')([{'general': {'login': lead_login}}])).all()

    if len(customers) > 1:
        logger.warning(f"Lead {lead_login} has more than one abccustomer")
        return None
    if not customers:
        return None

    return customers[0]


def get_abccustomer_by_amocustomer_login(customer_login: str) -> Optional[AbstractCustomer]:
    customers = db.session.query(AbstractCustomer).filter(
        AbstractCustomer.amo_customers.op('@>')([{'general': {'login': customer_login}}])).all()

    if len(customers) > 1:
        logger.warning(f"Customer {customer_login} has more than one abccustomer")
        return None
    if not customers:
        return None

    return customers[0]


def create_or_update_customer(abstract_customer: Optional[AbstractCustomer], amo_customer: Customer):
    if abstract_customer is None:
        new_abstract_customer = AbstractCustomer(
            amo_customers=[get_customer_json(amo_customer)],
        )
        db.session.add(new_abstract_customer)
        return new_abstract_customer

    abc_amo_customers = abstract_customer.amo_customers
    if not abc_amo_customers:
        # Не было покупателей
        abstract_customer.amo_customers = [get_customer_json(amo_customer)]
        return abstract_customer

    index = [idx for idx, c in enumerate(abstract_customer.amo_customers) if c['general']['id'] == amo_customer.id]
    if not index:
        index = len(abstract_customer.amo_customers) + 1
    else:
        index = index[0]

    update_jsonb_field(entity_id=abstract_customer.id,
                       attribute=AbcCustomerEntities.AmoCustomer,
                       index=index,
                       new_value=get_customer_json(amo_customer))
    return abstract_customer


def create_or_update_lead(abc_customer: Optional[AbstractCustomer], amo_lead: Lead) -> AbstractCustomer:
    if abc_customer is None:
        new_abstract_customer = AbstractCustomer(
            amo_leads=[get_lead_json(amo_lead)])
        db.session.add(new_abstract_customer)
        db.session.commit()
        return new_abstract_customer

    if not abc_customer.amo_leads:
        abc_customer.amo_leads = [get_lead_json(amo_lead)]
        db.session.commit()
        return abc_customer

    index = [idx for idx, c in enumerate(abc_customer.amo_leads) if c['general']['id'] == amo_lead.id]
    if not index:
        index = len(abc_customer.amo_leads) + 1
    else:
        index = index[0]

    update_jsonb_field(entity_id=abc_customer.id,
                       attribute=AbcCustomerEntities.AmoLead,
                       index=index,
                       new_value=get_lead_json(amo_lead))
    db.session.commit()
    return abc_customer
