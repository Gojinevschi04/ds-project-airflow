CREATE TABLE IF NOT EXISTS covid
    (
        id bigserial NOT NULL
            CONSTRAINT covid_pk
                PRIMARY KEY,
        date date NOT NULL,
        last_update timestamp without time zone NOT NULL,
        confirmed integer NOT NULL,
        confirmed_diff integer NOT NULL,
        deaths integer NOT NULL,
        deaths_diff integer NOT NULL,
        recovered integer NOT NULL,
        recovered_diff integer NOT NULL,
        active integer NOT NULL,
        active_diff integer NOT NULL,
        fatality_rate real NOT NULL,
        country_id integer NOT NULL
            CONSTRAINT covid_regions_id_fk REFERENCES regions (id),
        CONSTRAINT covid_pk_name_iso
            UNIQUE (country_id, date)
    );