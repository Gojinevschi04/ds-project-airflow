CREATE TABLE IF NOT EXISTS weather
    (
        id bigserial NOT NULL
            CONSTRAINT weather_pk
                PRIMARY KEY,
        date date NOT NULL,
        country_id integer NOT NULL
            CONSTRAINT weather_regions_id_fk REFERENCES regions (id),
        maxtemp_c real NOT NULL,
        maxtemp_f real NOT NULL,
        mintemp_c real NOT NULL,
        mintemp_f real NOT NULL,
        avgtemp_c real NOT NULL,
        avgtemp_f real NOT NULL,
        maxwind_mph real NOT NULL,
        maxwind_kph real NOT NULL,
        totalprecip_mm real NOT NULL,
        totalprecip_in real NOT NULL,
        totalsnow_cm real NOT NULL,
        avgvis_km real NOT NULL,
        avgvis_miles real NOT NULL,
        avghumidity real NOT NULL,
        daily_will_it_rain real NOT NULL,
        daily_chance_of_rain real NOT NULL,
        daily_will_it_snow real NOT NULL,
        daily_chance_of_snow real NOT NULL,
        uv real NOT NULL,
        CONSTRAINT weather_pk_name_iso
            UNIQUE (country_id, date)
    );