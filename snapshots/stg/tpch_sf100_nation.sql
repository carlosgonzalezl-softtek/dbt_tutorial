{% snapshot tpch_sf100_nation %}

{{
    config(
        strategy='check',
        unique_key='n_nationkey',
        check_cols='all'
    )
}}

SELECT * FROM {{source("tpch_sf100","nation")}}

{% endsnapshot %}