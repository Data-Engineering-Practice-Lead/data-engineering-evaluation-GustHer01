USE DATABASE staging;

CREATE OR REPLACE TABLE Stg_calendar(calendar variant);
DESC TABLE Stg_calendar;

CREATE OR REPLACE Stage calendar_stage url = 's3://staging-j/calendar.json';
COPY INTO Stg_calendar FROM @calendar_stage
    file_format = (type = json);

select
    value:date::string,
    value:year::int
    from
        Stg_calendar
    ,   lateral flatten(input => calendar);

select * from Stg_calendar;