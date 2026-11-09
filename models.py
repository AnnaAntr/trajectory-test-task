from enum import Enum

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, func, MetaData
from sqlalchemy.dialects.postgresql import JSONB

naming_convention = {
    "ix": 'ix_%(column_0_label)s',
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(column_0_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s"
}
db = SQLAlchemy(metadata=MetaData(naming_convention=naming_convention))


class AbcCustomerEntities(Enum):
    AmoCustomer = 'amo_customers'
    AmoLead = 'amo_leads'
    AmoCompany = 'amo_companies'
    AmoContact = 'amo_contacts'
    SendMethods = 'send_methods'


class Customer(db.Model):
    """
    Таблица для покупателя.
    jsonb_data - ответ CRM
    основные используемые поля из ответа записываются в поля модели
    """
    __tablename__ = "customer"
    id = Column(db.BigInteger(), nullable=False, primary_key=True)
    crm_customer_id = Column(db.BigInteger(), nullable=False, unique=True)
    login = Column(db.Text(), nullable=False)
    name = Column(db.Text())
    jsonb_data = Column(JSONB)
    send_methods = Column(JSONB)
    created_at = Column(db.DateTime, server_default=func.now())
    updated_at = Column(db.DateTime, onupdate=func.now())


class Lead(db.Model):
    """
    Таблица для сделки.
    Аналогично Customer
    """
    __tablename__ = "lead"
    id = Column(db.BigInteger(), nullable=False, primary_key=True)
    crm_lead_id = Column(db.BigInteger(), nullable=False, unique=True)
    login = Column(db.Text())
    name = Column(db.Text())
    # связь с покупателем
    customer_id = Column(db.BigInteger(), db.ForeignKey("customer.id", ondelete="CASCADE"), nullable=False)
    jsonb_data = Column(JSONB)
    created_at = Column(db.DateTime, server_default=func.now())
    updated_at = Column(db.DateTime, onupdate=func.now())


class AbstractCustomer(db.Model):
    """
    "default" "general" : {
        "id": int
    }

    "amo_customers": [
        {
            "general": {
                "login": str,
                "billing_type": str,
                "is_robot": bool,
                "is_nds": bool,
                "system": str,
                "host": str,
                "name": str
            },
            "api_value": {}
        }
    ]

    "amo_leads": [
        {
            "general": {
                "login": Optional[str],
                "name": str
            }
            "api_value": {}
        }
    ]

    "amo_companies": [
        {
            "general": {
                "id": int,
                "inn": Optional[str],
                "name": str
            }
            "api_value": {}
        }
    ]

    "amo_contacts": [
        {
            "general": {
                "id": int,
                "phone": Optional[str],
                "name": str
            }
            "api_value": {}
        }
    ]

    "send_methods": [
        {
            "type": str (mail,sms,wtsp),
            "value": str,
            "desc": str,
        }
    ]
    """
    __tablename__ = "abstract_customer"
    id = Column(db.BigInteger(), nullable=False, primary_key=True)
    amo_customers = Column(JSONB)
    amo_leads = Column(JSONB)
    amo_companies = Column(JSONB)
    amo_contacts = Column(JSONB)
    send_methods = Column(JSONB)
    created_at = Column(db.DateTime, server_default=func.now())
    updated_at = Column(db.DateTime, onupdate=func.now())
