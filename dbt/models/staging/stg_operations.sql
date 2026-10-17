-- Денежные операции — финальный статус
{{ cdc_current('operations', {'op_id': 'UUID', 'kind': 'String', 'status': 'String', 'initiator_user_id': 'UInt64', 'amount': 'Decimal(2)', 'fee': 'Decimal(2)', 'from_account': 'UInt64', 'to_account': 'UInt64', 'merchant_id': 'UInt64', 'parent_op_id': 'UUID', 'failure_reason': 'String', 'created_at': 'DateTime64', 'updated_at': 'DateTime64'}) }}
