select
    i.tanggal,
    l.lokasi_id,
    i.aqi_us,
    i.polutan_utama,
    i.suhu_celcius,
    i.kelembapan_persen,
    i.kecepatan_angin_ms,
    i.tren_sesak_napas,
    i.tren_obat_batuk,
    i.tren_gejala_dbd,
    i.status_risiko_udara
from {{ ref('int_health_correlation') }} i
left join {{ ref('dim_lokasi') }} l
    on i.lokasi = l.nama_lokasi