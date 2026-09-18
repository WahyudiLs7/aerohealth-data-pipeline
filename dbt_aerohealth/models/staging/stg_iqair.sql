with source as (
    select * from {{ source('public_schema', 'raw_iqair') }}
),

renamed as (
    select
        cast(tanggal_ekstraksi as date) as tanggal,
        lokasi,
        cast(aqi_us as integer) as aqi_us,
        polutan_utama,
        cast(suhu_celcius as integer) as suhu_celcius,
        cast(kelembapan_persen as integer) as kelembapan_persen,
        cast(kecepatan_angin_ms as numeric) as kecepatan_angin_ms
    from source
)

select * from renamed