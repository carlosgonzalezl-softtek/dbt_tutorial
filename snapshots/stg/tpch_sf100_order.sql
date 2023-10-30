{% snapshot tpch_sf100_order %}

{{
    config(
        strategy='timestamp',
        unique_key='o_orderkey',
        updated_at='o_orderDate'
    )
}}

SELECT *
FROM {{source("tpch_sf100","orders")}}
WHERE O_ORDERDATE >= CURRENT_DATE() - 30

{% endsnapshot %}