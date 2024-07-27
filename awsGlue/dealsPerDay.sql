--Deals per day
select
    year(datareferencia) as year,
    month(datareferencia) as month,
    day(datareferencia) as day,
    codigoinstrumento as codigoInstrumento,
    sum(replace(preconegocio, ',', '.') * quantidadenegociada) as precoTotal
from s3B3
group by datareferencia, codigoinstrumento