CREATE OR REPLACE TABLE covid_states(
    name STRING,
    state_code STRING,
    current_url STRING
);
CREATE OR REPLACE TABLE Dim_states(
    name STRING,
    state_code STRING,
    current_url STRING
);
insert into covid_states
    select
        value:name::string,
        value:state_code::string,
        value:current_url::string
        from
            Stg_covid_states
        , lateral flatten(input => states_json);

CREATE OR REPLACE PROCEDURE dimensioning()
    returns varchar
    language javascript
    as
    $$
    snowflake.execute({ sqlText:
        `
            merge into dim_states dim
            using (select * from covid_states) src
            on dim.name = src.name
            when matched and (dim.state_code != src.state_code or dim.current_url != src.current_url)
            then
            update set dim.name = src.name
            when not matched
            then
            insert(name, state_code, current_url) values(src.name, src.state_code, src.current_url);

        `
    });
    return 'Done';
    $$
    ;
call dimensioning();