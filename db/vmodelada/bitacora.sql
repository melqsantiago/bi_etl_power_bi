-- Table: vmodelada.etl_bitacora

-- DROP TABLE IF EXISTS vmodelada.etl_bitacora;

CREATE TABLE IF NOT EXISTS vmodelada.etl_bitacora
(
    id_log integer NOT NULL DEFAULT nextval('vmodelada.etl_bitacora_id_log_seq'::regclass),
    nombre_proceso character varying(255) COLLATE pg_catalog."default" NOT NULL,
    fecha_ejecucion timestamp with time zone DEFAULT now(),
    estado character varying(50) COLLATE pg_catalog."default" NOT NULL,
    registros_cargados integer,
    mensaje_error text COLLATE pg_catalog."default",
    CONSTRAINT etl_bitacora_pkey PRIMARY KEY (id_log)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS vmodelada.etl_bitacora
    OWNER to postgres;