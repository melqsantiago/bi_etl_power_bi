-- Table: vmodelada.dim_victima

-- DROP TABLE IF EXISTS vmodelada.dim_victima;

CREATE TABLE IF NOT EXISTS vmodelada.dim_victima
(
    id_victima integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    nombres character varying(100) COLLATE pg_catalog."default",
    apellidos character varying(100) COLLATE pg_catalog."default",
    edad integer,
    sexo character varying(8) COLLATE pg_catalog."default",
    estado_civil character varying(8) COLLATE pg_catalog."default",
    id_direccion integer,
    victima_f_original bigint,
    CONSTRAINT dim_victima_pkey PRIMARY KEY (id_victima),
    CONSTRAINT dim_victima_victima_f_original_key UNIQUE (victima_f_original)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS vmodelada.dim_victima
    OWNER to postgres;