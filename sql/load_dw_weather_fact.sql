INSERT INTO dbo.dw_covid_weather_fact (unique_id,
                                       country_id,
                                       date,
                                       confirmed,
                                       deaths,
                                       recovered,
                                       active,
                                       fatality_rate,
                                       average_temperature_c,
                                       average_humidity,
                                       total_precipitation_mm)
SELECT
    -- Generăm unique_id
    MD5(CAST(c.country_id AS TEXT) || '_' || CAST(c.date AS TEXT)) AS unique_id, c.country_id, c.date,
    -- COVID data
    c.confirmed, c.deaths, c.recovered, c.active, c.fatality_rate,
    -- WEATHER data
    w.avgtemp_c, w.avghumidity, w.totalprecip_mm
  FROM dbo.covid c
           JOIN dbo.weather w
           ON c.country_id = w.country_id AND c.date = w.date - INTERVAL '5 years'
    ON CONFLICT (unique_id) DO UPDATE SET confirmed = excluded.confirmed,
                                          deaths = excluded.deaths,
                                          recovered = excluded.recovered,
                                          active = excluded.active,
                                          fatality_rate = excluded.fatality_rate,
                                          average_temperature_c = excluded.average_temperature_c,
                                          average_humidity = excluded.average_humidity,
                                          total_precipitation_mm = excluded.total_precipitation_mm;
