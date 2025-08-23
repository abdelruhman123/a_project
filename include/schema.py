# Schema definitions

ORDER_PAYMENTS_SCHEMA = [
    {"name": "order_id",             "type": "STRING",  "mode": "REQUIRED"},
    {"name": "payment_sequential",   "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "payment_type",         "type": "STRING",  "mode": "NULLABLE"},
    {"name": "payment_installments", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "payment_value",        "type": "FLOAT",   "mode": "NULLABLE"},
]

SELLERS_SCHEMA = [
    {"name": "seller_id",             "type": "STRING",  "mode": "REQUIRED"},
    {"name": "seller_zip_code_prefix","type": "INTEGER", "mode": "NULLABLE"},
    {"name": "seller_city",           "type": "STRING",  "mode": "NULLABLE"},
    {"name": "seller_state",          "type": "STRING",  "mode": "NULLABLE"},
]