-- Все версии строк пользователей из журнала CDC — основа SCD2
{{ cdc_versions('users', {'user_id': 'UInt64', 'created_at': 'DateTime64', 'updated_at': 'DateTime64', 'birth_year': 'UInt16', 'city': 'String', 'channel': 'String', 'kyc_level': 'String', 'status': 'String'}) }}
