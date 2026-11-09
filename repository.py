from typing import Dict, Optional
from sqlalchemy import func, select
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
