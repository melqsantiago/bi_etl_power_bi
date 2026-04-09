-- Table: vmodelada.hechos_casos

-- DROP TABLE IF EXISTS vmodelada.hechos_casos;

CREATE TABLE IF NOT EXISTS vmodelada.hechos_casos
(
    id_caso integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    id_victima integer,
    id_violencia integer,
    id_direccion integer,
    id_tiempo integer,
    reincidencia_casos integer,
    numero_casos integer,
    CONSTRAINT hechos_casos_pkey PRIMARY KEY (id_caso),
    CONSTRAINT hechos_casos_id_tiempo_fkey FOREIGN KEY (id_tiempo)
        REFERENCES vmodelada.dim_tiempo (id_tiempo) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT id_direccion FOREIGN KEY (id_direccion)
        REFERENCES vmodelada.dim_direccion (id_direccion) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT id_victima FOREIGN KEY (id_victima)
        REFERENCES vmodelada.dim_victima (id_victima) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT id_violencia FOREIGN KEY (id_violencia)
        REFERENCES vmodelada.dim_tipo_violencia (id_violencia) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS vmodelada.hechos_casos
    OWNER to postgres;