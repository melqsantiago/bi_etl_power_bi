-- Table: vmodelada.dim_direccion

-- DROP TABLE IF EXISTS vmodelada.dim_direccion;

CREATE TABLE IF NOT EXISTS vmodelada.dim_direccion
(
    id_direccion integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    cnegociod integer,
    nom_depto character varying(100) COLLATE pg_catalog."default",
    cnegociom integer,
    nom_muni character varying(100) COLLATE pg_catalog."default",
    CONSTRAINT dim_direccion_pkey PRIMARY KEY (id_direccion)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS vmodelada.dim_direccion
    OWNER to postgres;