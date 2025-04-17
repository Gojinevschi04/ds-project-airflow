CREATE TABLE IF NOT EXISTS dbo.dw_covid_weather_fact
    (
        unique_id text
            PRIMARY KEY,
        country_id integer NOT NULL
            CONSTRAINT dw_regions_id_fk REFERENCES dbo.regions (id),
        date date NOT NULL,

        -- COVID
        confirmed integer,
        deaths integer,
        recovered integer,
        active integer,
        fatality_rate real,

        -- WEATHER
        average_temperature_c real,
        average_humidity real,
        total_precipitation_mm real,

        CONSTRAINT unique_country_date
            UNIQUE (country_id, date)
    );