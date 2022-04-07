CREATE OR REPLACE TABLE calendar(
    id_date int autoincrement primary key,
    date date,
    day int,
    month int,
    year int
);

INSERT INTO calendar(date, day, month, year)
    select
        value:date::string,
        value:day::int,
        value:month::int,
        value:year::int
    from
        Stg_calendar
    ,
        lateral flatten(input => calendar);

select * from calendar;