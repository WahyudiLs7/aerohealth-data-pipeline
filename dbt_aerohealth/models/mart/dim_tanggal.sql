with tanggal_unik as (
    select distinct tanggal
    from {{ ref('int_health_correlation') }}
)

select
    tanggal,
    extract(day from tanggal) as hari,
    extract(month from tanggal) as bulan,
    extract(year from tanggal) as tahun
from tanggal_unik