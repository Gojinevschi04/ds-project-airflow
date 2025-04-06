CREATE TABLE IF NOT EXISTS dbo.log_transform
    (
        id serial NOT NULL
            CONSTRAINT transform_log_pk
                PRIMARY KEY,
        batch_date date NOT NULL,
        country_id integer NOT NULL
            CONSTRAINT regions_id_fk REFERENCES dbo.regions (id),
        processed_directory_name varchar(100) NOT NULL,
        processed_file_name varchar(100) NOT NULL,
        rows_count smallint NOT NULL,
        status varchar(100) NOT NULL
    );