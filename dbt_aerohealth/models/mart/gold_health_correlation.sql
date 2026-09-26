select
    f.tanggal,
    d.hari,
    d.bulan,
    d.tahun,
    l.nama_lokasi,
    l.provinsi,
    f.aqi_us,
    f.polutan_utama,
    f.suhu_celcius,
    f.kelembapan_persen,
    f.kecepatan_angin_ms,
    f.tren_sesak_napas,
    f.tren_obat_batuk,
    f.tren_gejala_dbd,
    f.status_risiko_udara
from {{ ref('fact_health_correlation') }} f
left join {{ ref('dim_tanggal') }} d on f.tanggal = d.tanggal
left join {{ ref('dim_lokasi') }} l on f.lokasi_id = l.lokasi_id