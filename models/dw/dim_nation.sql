{{
    config(
        materialized = 'incremental',
        unique_key = 'nation_id'
    )
}}

WITH FINAL AS(
    SELECT
        md5(nat.n_nationkey)                            as nation_id,
        nat.n_nationkey                                 as nation_src_num,
        md5(nat.n_regionkey)                            as region_id,
        nat.n_regionkey                                 as region_src_num,

        nat.n_name                                      as nation_name,
        nat.n_comment                                   as nation_comment,
        {% if is_incremental() %}
        nvl(this.audt_create_date, current_timestamp()) as audt_create_date,
        {% else %}
        current_timestamp()                             as audt_create_date,
        {% endif %}
        current_timestamp()                             as audt_update_date
    FROM {{ref("tpch_sf100_nation")}} nat
    {% if is_incremental() %}
    LEFT JOIN {{this}} as this on nat.n_nationkey = this.nation_src_num
    {% endif %}
    WHERE nat.dbt_valid_to is null
)
SELECT * FROM FINAL