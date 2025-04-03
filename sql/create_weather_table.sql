create table if not exists weather
(
    id                   bigserial not null
        constraint weather_pk
            primary key,
    date                 date      not null,
    country_id           integer   not null
        constraint weather_regions_id_fk
            references regions (id),
    maxtemp_c            real      not null,
    maxtemp_f            real      not null,
    mintemp_c            real      not null,
    mintemp_f            real      not null,
    avgtemp_c            real      not null,
    avgtemp_f            real      not null,
    maxwind_mph          real      not null,
    maxwind_kph          real      not null,
    totalprecip_mm       real      not null,
    totalprecip_in       real      not null,
    totalsnow_cm         real      not null,
    avgvis_km            real      not null,
    avgvis_miles         real      not null,
    avghumidity          real      not null,
    daily_will_it_rain   real      not null,
    daily_chance_of_rain real      not null,
    daily_will_it_snow   real      not null,
    daily_chance_of_snow real      not null,
    uv                   real      not null
);