create table if not exists covid
(
    id             bigserial                   not null
        constraint covid_pk
            primary key,
    date           date                        not null,
    last_update    timestamp without time zone not null,
    confirmed      integer                     not null,
    confirmed_diff integer                     not null,
    deaths         integer                     not null,
    deaths_diff    integer                     not null,
    recovered      integer                     not null,
    recovered_diff integer                     not null,
    active         integer                     not null,
    active_diff    integer                     not null,
    fatality_rate  real                        not null,
    country_id     integer                     not null
        constraint covid_regions_id_fk
            references regions (id)
);