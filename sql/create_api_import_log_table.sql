CREATE TABLE IF NOT EXISTS dbo.log_api_import
    (
        id serial NOT NULL
            CONSTRAINT api_import_log_pk
                PRIMARY KEY,
        country_id integer NOT NULL
            CONSTRAINT regions_id_fk REFERENCES regions (id),
        start_time timestamp without time zone NOT NULL,
        end_time timestamp without time zone NOT NULL,
        code_response smallint NOT NULL,
        error_messages text
    );