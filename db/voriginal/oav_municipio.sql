-- Table: voriginal.oav_renap_municipio

-- DROP TABLE IF EXISTS voriginal.oav_renap_municipio;

CREATE TABLE IF NOT EXISTS voriginal.oav_renap_municipio
(
    id_depto bigint,
    id_muni bigint,
    descripcion text COLLATE pg_catalog."default",
    usuario_ultima_mod text COLLATE pg_catalog."default",
    fh_ultima_mod text COLLATE pg_catalog."default",
    id_depto_muni bigint,
    ip_ultima_mod text COLLATE pg_catalog."default",
    fh_ingreso text COLLATE pg_catalog."default",
    usuario_ingreso text COLLATE pg_catalog."default",
    ip_ingreso text COLLATE pg_catalog."default",
    id_codigo_ine double precision
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS voriginal.oav_renap_municipio
    OWNER to postgres;