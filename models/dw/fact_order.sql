{{
    config(
        materialized='incremental',
        unique_key='order_src_num'
    )
}}

WITH FINAL AS(
    SELECT
        ORD.O_ORDERKEY                                        AS ORDER_SRC_NUM,
        ORD.O_ORDERSTATUS                                     AS ORDER_STATUS,
        ORD.O_TOTALPRICE                                      AS ORDER_PRICE,
        ORD.O_ORDERDATE                                       AS ORDER_DATE,
        {% if is_incremental() %}
            NVL(FO.AUDT_CREATE_DATE, current_timestamp())     AS AUDT_CREATE_DATE,
        {% else %}
            current_timestamp()                               AS AUDT_CREATE_DATE,
        {% endif %}
        current_timestamp()                                   AS AUDT_UPDATE_DATE
    FROM {{ref('tpch_sf100_order')}} ORD
    {% if is_incremental() %}
        LEFT JOIN {{this}} FO on ORD.O_ORDERKEY = FO.ORDER_SRC_NUM
    {% endif %}
    WHERE ord.dbt_valid_to is null
    {% if is_incremental() %}
    AND ORD.O_ORDERDATE > (SELECT MAX(ORDER_DATE) - 2 FROM {{this}} )
    {% endif %}
)
SELECT *
FROM FINAL