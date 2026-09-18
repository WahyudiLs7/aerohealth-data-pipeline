with source as (
    select * from {{ source('public_schema', 'raw_google_trends') }}
),

renamed as (
    select
        cast(tanggal_pencarian as date) as tanggal,
        cast("sesak napas" as integer) as tren_sesak_napas,
        cast("obat batuk" as integer) as tren_obat_batuk,
        cast("gejala dbd" as integer) as tren_gejala_dbd
    from source
)

select * from renamed