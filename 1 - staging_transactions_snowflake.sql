USE DATABASE staging;

CREATE OR REPLACE TABLE Stg_covid_transactions(states_json variant);
desc table Stg_covid_transactions;
CREATE OR REPLACE STAGE json_stage url = 's3://staging-j/covid_transactions.json';

LIST @json_stage;

COPY INTO Stg_covid_transactions FROM @json_stage
    file_format = (type = json);

select
  value:name::string,
  value:state_code::string
  from
    Stg_covid_transactions
  , lateral flatten(input => states_json);