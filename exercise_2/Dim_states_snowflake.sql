CREATE OR REPLACE TABLE covid_states(
    id_state int autoincrement primary key,
    state STRING,
    state_code STRING
);
CREATE OR REPLACE TABLE Dim_states(
    name STRING,
    state_code STRING,
    current_url STRING
);

insert into covid_states(state, state_code)
    select
        states_parquet['name'],
        states_parquet['state_code']
    from Stg_covid_states;