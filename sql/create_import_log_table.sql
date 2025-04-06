CREATE TABLE IF NOT EXISTS dbo.log_import
    (
        id serial NOT NULL
            CONSTRAINT import_log_pk
                PRIMARY KEY,
        batch_date date NOT NULL,
        country_id integer NOT NULL
            CONSTRAINT regions_id_fk REFERENCES dbo.regions (id),
        import_directory_name varchar(100) NOT NULL,
        import_file_name varchar(100) NOT NULL,
        file_created_date timestamp without time zone NOT NULL,
        file_last_modified_date timestamp without time zone NOT NULL,
        rows_count smallint NOT NULL
    );