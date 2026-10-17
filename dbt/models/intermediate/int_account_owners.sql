-- Кому принадлежит счёт: пользователь, мерчант или системный счёт
select a.account_id, a.owner_type, a.user_id, m.merchant_id, a.code
from {{ ref('stg_accounts') }} a
left join {{ ref('stg_merchants') }} m on m.account_id = a.account_id
