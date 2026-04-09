-- Table: vmodelada.dim_tiempo

-- DROP TABLE IF EXISTS vmodelada.dim_tiempo;

CREATE TABLE IF NOT EXISTS vmodelada.dim_tiempo
(
    id_tiempo integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    fecha date,
    dia integer,
    mes integer,
    "año" integer,
    trimestre integer,
    nombre_mes character varying(50) COLLATE pg_catalog."default",
    nombre_dia character varying(20) COLLATE pg_catalog."default",
    CONSTRAINT dim_tiempo_pkey PRIMARY KEY (id_tiempo)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS vmodelada.dim_tiempo
    OWNER to postgres;