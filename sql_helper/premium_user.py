from collections import namedtuple
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

from .premium_last_charge_status import PremiumLastChargeStatus


ALLOWED_LAST_CHARGE_STATUSES = (
    PremiumLastChargeStatus.PAID,
    PremiumLastChargeStatus.PENDING,
    PremiumLastChargeStatus.DELETED
)


class PremiumUser(namedtuple(
    "PremiumUser",
    ["patreon_id", "discord_patreon_id", "lifetime_support_cents", "last_charge_date", "last_charge_status", "tokens", "tokens_spent", "pledge_id", "discord_override_id"]
)):
    def __new__(cls, *args, **kwargs):
        if args:
            args = list(args)
            args[4] = PremiumLastChargeStatus(args[4])
        if kwargs:
            kwargs["last_charge_date"] = kwargs.get("last_charge_date") and kwargs["last_charge_date"].date()
        return super(PremiumUser, cls).__new__(cls, *args, **kwargs)

    @property
    def discord_id(self):
        return self.discord_override_id or self.discord_patreon_id

    def should_have_premium(self, discord_requirement: bool = True) -> bool:
        return (
            (self.discord_id or not discord_requirement) and
            self.last_charge_status in ALLOWED_LAST_CHARGE_STATUSES and
            self.last_charge_date + relativedelta(months=1) >= date.today()
        )
