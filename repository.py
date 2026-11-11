from typing import Dict, Optional
from sqlalchemy import delete, func, select, text, update
from sqlalchemy.dialects.postgresql import insert

from models import Customer, Lead, db


def upsert_customers(rows, run_id):
    """
    Пакетная запись покупателей, один  UPSERT на весь пакет

    rows - список словарей с ключами crm_customer_id, login, name, jsonb_data, send_methods
    run_id - id прогона, записывается в last_seen_run_id
    """
    if not rows:
        return 0
    values = [
        {
            "crm_customer_id": row["crm_customer_id"],
            "login": row["login"],
            "name": row.get("name"),
            "jsonb_data": row.get("jsonb_data"),
            "send_methods": row.get("send_methods"),
            "last_seen_run_id": run_id,
        }
        for row in rows
    ]
    query = insert(Customer).values(values)
    query = query.on_conflict_do_update(
        index_elements=[Customer.crm_customer_id],
        set_={
            "login": query.excluded.login,
            "name": query.excluded.name,
            "jsonb_data": query.excluded.jsonb_data,
            "send_methods": query.excluded.send_methods,
            "last_seen_run_id": query.excluded.last_seen_run_id,
            "updated_at": func.now(),
        },
    )
    db.session.execute(query)


def upsert_leads(rows, run_id):
    """
    Аналогично пользователям - пакетная запись сделок, один  UPSERT на весь пакет

    rows - список словарей с ключами crm_lead_id, login, name, customer_id, jsonb_data
    run_id - id прогона, записывается в last_seen_run_id
    """
    if not rows:
        return 0
    values = [
        {
            "crm_lead_id": row["crm_lead_id"],
            "login": row.get("login"),
            "name": row.get("name"),
            "customer_id": row.get("customer_id"),
            "jsonb_data": row.get("jsonb_data"),
            "last_seen_run_id": run_id,
        }
        for row in rows
    ]
    query = insert(Lead).values(values)
    query = query.on_conflict_do_update(
        index_elements=[Lead.crm_lead_id],
        set_={
            "login": query.excluded.login,
            "name": query.excluded.name,
            "customer_id": query.excluded.customer_id,
            "jsonb_data": query.excluded.jsonb_data,
            "last_seen_run_id": query.excluded.last_seen_run_id,
            "updated_at": func.now(),
        },
    )
    db.session.execute(query)


def load_customers_data():
    """
    Загружает из БД в словарь данные пользователей
    Получается один SELECT по id/crm_customer_id/login вместо запроса на каждого покупателя
    """
    by_crm_id: Dict[int, int] = {}
    by_login: Dict[str, Optional[int]] = {}
    rows = db.session.execute(select(Customer.id, Customer.crm_customer_id, Customer.login))
    for customer_id, crm_customer_id, login in rows:
        by_crm_id[crm_customer_id] = customer_id
        if not login:
            continue
        if login in by_login and by_login[login] != customer_id:
            by_login[login] = None
        else:
            by_login[login] = customer_id
    return by_crm_id, by_login


def load_leads_data():
    """
    Загружает из БД в словарь данные сделок
    Получается один SELECT по id/crm_lead_id/login/customer_id вместо запроса на каждого покупателя
    """
    by_crm_id: Dict[int, tuple] = {}
    by_login: Dict[str, Optional[int]] = {}
    seen_logins = set()
    rows = db.session.execute(select(Lead.id, Lead.crm_lead_id, Lead.login, Lead.customer_id))
    for lead_id, crm_lead_id, login, customer_id in rows:
        by_crm_id[crm_lead_id] = (lead_id, customer_id)
        if not login:
            continue
        if login in seen_logins:
            by_login[login] = None
            continue
        seen_logins.add(login)
        by_login[login] = customer_id
    return by_crm_id, by_login


def merge_customers(first_id: int, second_id: int):
    """
    Мержит покупателя second в first
    """
    if first_id is None or second_id is None or first_id == second_id:
        return False

    rows = db.session.execute(
        select(Customer.id, Customer.jsonb_data, Customer.send_methods).where(Customer.id.in_([first_id, second_id]))
    ).all()

    first_data = None
    first_send = None
    second_data = None
    second_send = None
    found_first = False
    found_second = False

    for customer_id, jsonb_data, send_methods in rows:
        if customer_id == first_id:
            first_data = jsonb_data
            first_send = send_methods
            found_first = True
        elif customer_id == second_id:
            second_data = jsonb_data
            second_send = send_methods
            found_second = True

    if not found_first or not found_second:
        return False

    merged = dict(first_data or {})
    second_data = second_data or {}
    for key in ("contacts", "companies"):
        merged_list = []
        seen_ids = set()
        items = (merged.get(key) or []) + (second_data.get(key) or [])

        for item in items:
            item_id = item.get("id")
            if item_id in seen_ids:
                continue
            seen_ids.add(item_id)
            merged_list.append(item)

        merged[key] = merged_list

    merged_send = []
    seen_send = set()
    send_items = (first_send or []) + (second_send or [])

    for item in send_items:
        key = (item.get("type"), item.get("value"))
        if key in seen_send:
            continue
        seen_send.add(key)
        merged_send.append(item)

    db.session.execute(
        update(Lead).where(Lead.customer_id == second_id).values(customer_id=first_id)
    )
    db.session.execute(
        update(Customer).where(Customer.id == first_id).values(
            jsonb_data=merged,
            send_methods=merged_send or None,
            updated_at=func.now(),
        )
    )
    db.session.execute(delete(Customer).where(Customer.id == second_id))

    return True


def cleanup(run_id, contact_ids=None, company_ids=None) -> Dict[str, int]:
    """
    Удаляет сущности, которых не было в текущем прогоне (last_seen_run_id не равен run_id)

    contact_ids/company_ids — id контактов и компаний, встреченных за прогон
    Из jsonb_data покупателя вычищаются вложенные элементы не из этих списков и любые элементы с «удалить» в имени
    Если списки не переданы, вложенные элементы не трогаются

    Возвращает количество изменённых строк по покупателям, сделкам и вложенным элементам
    """
    deleted_leads = db.session.execute(
        delete(Lead).where(Lead.last_seen_run_id.is_distinct_from(run_id))
    ).rowcount
    deleted_customers = db.session.execute(
        delete(Customer).where(Customer.last_seen_run_id.is_distinct_from(run_id))
    ).rowcount

    updated_customers = 0
    if contact_ids is not None:
        query = text("""
            UPDATE customer
            SET jsonb_data = jsonb_set(
                    jsonb_data,
                    '{contacts}',
                    COALESCE((
                        SELECT jsonb_agg(item)
                        FROM jsonb_array_elements(jsonb_data->'contacts') AS item
                        WHERE (item->>'id')::bigint = ANY(CAST(:contact_ids AS bigint[]))
                          AND strpos(lower(COALESCE(item->>'name', '')), 'удалить') = 0
                    ), '[]'::jsonb)
                ),
                updated_at = now()
            WHERE last_seen_run_id = :run_id
              AND jsonb_typeof(jsonb_data->'contacts') = 'array'
        """)
        updated_customers += db.session.execute(query, {
            'run_id': run_id,
            'contact_ids': list(contact_ids),
        }).rowcount

    if company_ids is not None:
        query = text("""
            UPDATE customer
            SET jsonb_data = jsonb_set(
                    jsonb_data,
                    '{companies}',
                    COALESCE((
                        SELECT jsonb_agg(item)
                        FROM jsonb_array_elements(jsonb_data->'companies') AS item
                        WHERE (item->>'id')::bigint = ANY(CAST(:company_ids AS bigint[]))
                          AND strpos(lower(COALESCE(item->>'name', '')), 'удалить') = 0
                    ), '[]'::jsonb)
                ),
                updated_at = now()
            WHERE last_seen_run_id = :run_id
              AND jsonb_typeof(jsonb_data->'companies') = 'array'
        """)
        updated_customers += db.session.execute(query, {
            'run_id': run_id,
            'company_ids': list(company_ids),
        }).rowcount

    return {'leads': deleted_leads, 'customers': deleted_customers, 'nested': updated_customers}
