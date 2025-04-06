CREATE TABLE IF NOT EXISTS regions
    (
        id serial NOT NULL
            CONSTRAINT regions_pk
                PRIMARY KEY,
        code varchar(20) NOT NULL,
        name varchar(100) NOT NULL,
        CONSTRAINT regions_pk_name_iso
            UNIQUE (name, code)
    );