-- Заявки KYC
{{ cdc_current('kyc_checks', {'kyc_id': 'UInt64', 'user_id': 'UInt64', 'level': 'String', 'status': 'String', 'submitted_at': 'DateTime64', 'decided_at': 'DateTime64', 'reject_reason': 'String'}) }}
