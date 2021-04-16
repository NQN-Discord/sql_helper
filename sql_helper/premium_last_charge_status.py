from enum import Enum


class PremiumLastChargeStatus(Enum):
    PAID = "Paid"
    DECLINED = "Declined"
    DELETED = "Deleted"
    PENDING = "Pending"
    REFUNDED = "Refunded"
    FRAUD = "Fraud"
    OTHER = "Other"
