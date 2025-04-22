CREATE TABLE IF NOT EXISTS dbo.visualization_covid_weather_fact
    (
        unique_id text
            PRIMARY KEY,
        country_id integer NOT NULL
            CONSTRAINT dw_visualization_regions_id_fk REFERENCES dbo.regions (id),
        date date NOT NULL,

        real_value real,
        prediction real,

        CONSTRAINT unique_country_date_visualization
            UNIQUE (country_id, date)
    );