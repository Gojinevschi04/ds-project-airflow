create table if not exists regions
(
    id   serial       not null
        constraint regions_pk primary key,
    code char(20)     not null,
    name varchar(100) not null
);