select
    year(datanegocio) as year,
    month(datanegocio) as month,
    day(datanegocio) as day,
    codigoparticipantecomprador as codigo_comprador,
    codigoinstrumento as codigo_instrumento,
    sum(quantidadenegociada) as quantidade_negociada
from myDataSource
group by datanegocio, codigoparticipantecomprador, codigoinstrumento
order by datanegocio, codigoparticipantecomprador