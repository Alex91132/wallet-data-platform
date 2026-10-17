-- Счета и их денормализованные остатки
{{ cdc_current('accounts', {'account_id': 'UInt64', 'owner_type': 'String', 'user_id': 'UInt64', 'code': 'String', 'balance': 'Decimal(2)', 'allow_negative': 'Bool', 'created_at': 'DateTime64'}) }}
