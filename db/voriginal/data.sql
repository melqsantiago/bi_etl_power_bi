-- Table: voriginal.data

-- DROP TABLE IF EXISTS voriginal.data;

CREATE TABLE IF NOT EXISTS voriginal.data
(
    num_caso bigint,
    id_caso bigint,
    fh_caso text COLLATE pg_catalog."default",
    dpi_victima double precision,
    nombres_victima text COLLATE pg_catalog."default",
    apellidos_victima text COLLATE pg_catalog."default",
    edad_victima double precision,
    fec_nac_victima text COLLATE pg_catalog."default",
    sexo_descripcion text COLLATE pg_catalog."default",
    alfabetismo_v double precision,
    ocupacion_victima double precision,
    relacion_vic_agresor bigint,
    estado_civil_victima bigint,
    etnia_victima bigint,
    "relacion_vic_agresor-2" bigint,
    direc_cod_depto_victima bigint,
    cod_depto_hecho bigint,
    cod_muni_hecho bigint,
    id_sede bigint,
    estacion text COLLATE pg_catalog."default",
    subestacion text COLLATE pg_catalog."default",
    tipo_hecho text COLLATE pg_catalog."default",
    tipo_agresion_descripcion text COLLATE pg_catalog."default",
    total_hijos bigint,
    direc_zona_victima bigint,
    "direc_cod_depto_victima-2" bigint,
    direc_cod_muni_victima bigint,
    discapacidad bigint,
    sexo_sindicado bigint,
    edad_sindicado double precision,
    fec_nac_sindicado text COLLATE pg_catalog."default",
    ocupacion_sindicado double precision,
    lugar_agresion text COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS voriginal.data
    OWNER to postgres;