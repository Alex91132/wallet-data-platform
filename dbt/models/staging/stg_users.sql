-- Пользователи — текущее состояние (без ПДн)
{{ cdc_current('users', {'user_id': 'UInt64', 'created_at': 'DateTime64', 'updated_at': 'DateTime64', 'birth_year': 'UInt16', 'city': 'String', 'channel': 'String', 'kyc_level': 'String', 'status': 'String'}) }}
