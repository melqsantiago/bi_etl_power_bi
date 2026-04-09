-- Table: voriginal.oav_renap_departamento

-- DROP TABLE IF EXISTS voriginal.oav_renap_departamento;

CREATE TABLE IF NOT EXISTS voriginal.oav_renap_departamento
(
    id_depto bigint,
    descripcion text COLLATE pg_catalog."default",
    usuario_ultima_mod text COLLATE pg_catalog."default",
    fh_ultima_mod text COLLATE pg_catalog."default",
    ip_ultima_mod text COLLATE pg_catalog."default",
    usuario_ingreso text COLLATE pg_catalog."default",
    fh_ingreso text COLLATE pg_catalog."default",
    ip_ingreso text COLLATE pg_catalog."default",
    id_codigo_ine bigint
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS voriginal.oav_renap_departamento
    OWNER to postgres;