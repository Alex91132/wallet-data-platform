-- Распределение пользователей по группам экспериментов
{{ cdc_current('experiment_assignments', {'experiment_key': 'String', 'user_id': 'UInt64', 'variant': 'String', 'assigned_at': 'DateTime64'}) }}
