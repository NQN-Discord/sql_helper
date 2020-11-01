from collections import namedtuple


PremiumUser = namedtuple(
    "PremiumUser",
    ["patreon_id", "discord_id", "lifetime_support_cents", "last_charge_date", "last_charge_status", "tokens", "tokens_spent"]
)
