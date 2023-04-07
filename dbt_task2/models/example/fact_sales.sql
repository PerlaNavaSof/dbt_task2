{{
    config(
        materialized='incremental',
        unique_key='update_src_date ',
        tags=['daily']
    )
}}

with src as(
    select *
    from {{ source('src_sal','SRC_SALES') }}
)

select *, current_timestamp as audt_update_date
from src

{% if is_incremental() %}
    where update_src_date > (select max(update_src_date) from {{this}})
{% endif %}