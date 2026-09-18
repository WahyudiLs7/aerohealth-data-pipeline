with iqair_agg as (
    select 
        tanggal,
        lokasi,
        MAX(polutan_utama) as polutan_utama,
        ROUND(AVG(aqi_us)) as aqi_us,
        ROUND(AVG(suhu_celcius)) as suhu_celcius,
        ROUND(AVG(kelembapan_persen)) as kelembapan_persen,
        ROUND(AVG(kecepatan_angin_ms), 1) as kecepatan_angin_ms
    from {{ ref('stg_iqair') }}
    group by 1, 2
),

trends_agg as (
    select 
        tanggal,
        ROUND(AVG(tren_sesak_napas)) as tren_sesak_napas,
        ROUND(AVG(tren_obat_batuk)) as tren_obat_batuk,
        ROUND(AVG(tren_gejala_dbd)) as tren_gejala_dbd
    from {{ ref('stg_google_trends') }}
    group by 1
),

final as (
    select
        t.tanggal,
        coalesce(i.lokasi, 'Pekanbaru') as lokasi,
        i.aqi_us,
        i.polutan_utama,
        i.suhu_celcius,
        i.kelembapan_persen,
        i.kecepatan_angin_ms,
        t.tren_sesak_napas,
        t.tren_obat_batuk,
        t.tren_gejala_dbd,
        case
            when i.aqi_us > 150 then 'Tinggi (Tidak Sehat)'
            when i.aqi_us > 100 then 'Sedang (Waspada)'
            when i.aqi_us is null then 'Data Cuaca Kosong'
            else 'Aman'
        end as status_risiko_udara
    from trends_agg t
    left join iqair_agg i on t.tanggal = i.tanggal
)

select * from final