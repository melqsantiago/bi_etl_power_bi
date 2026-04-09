-- Table: vmodelada.dim_tipo_violencia

-- DROP TABLE IF EXISTS vmodelada.dim_tipo_violencia;

CREATE TABLE IF NOT EXISTS vmodelada.dim_tipo_violencia
(
    id_violencia integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    nombre_violencia character varying(50) COLLATE pg_catalog."default",
    violencia_f_original character varying(50) COLLATE pg_catalog."default",
    CONSTRAINT dim_tipo_violencia_pkey PRIMARY KEY (id_violencia)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS vmodelada.dim_tipo_violencia
    OWNER to postgres;