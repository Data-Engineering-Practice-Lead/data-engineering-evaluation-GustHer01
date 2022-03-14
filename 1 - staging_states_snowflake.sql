CREATE OR REPLACE DATABASE staging;
USE DATABASE staging;

CREATE OR REPLACE TABLE Stg_covid_states(states_json variant);
desc table Stg_covid_states;
CREATE OR REPLACE STAGE json_stage url = 's3://staging-j/covid_states.json';

LIST @json_stage;

COPY INTO Stg_covid_states FROM @json_stage
    file_format = (type = json);

select
  value:name::string,
  value:state_code::string
  from
    Stg_covid_states
  , lateral flatten(input => states_json);