-- Проводки (двойная запись)
{{ cdc_current('ledger_entries', {'entry_id': 'UInt64', 'op_id': 'UUID', 'account_id': 'UInt64', 'amount': 'Decimal(2)', 'created_at': 'DateTime64'}) }}
