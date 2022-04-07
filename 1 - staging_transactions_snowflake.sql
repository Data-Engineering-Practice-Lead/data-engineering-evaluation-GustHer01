USE DATABASE staging;

CREATE OR REPLACE TABLE Stg_covid_transactions(states_json variant);
desc table Stg_covid_transactions;
CREATE OR REPLACE STAGE trans_stage url = 's3://staging-j/covid_transactions.json';

LIST @trans_stage;

COPY INTO Stg_covid_transactions FROM @trans_stage
    file_format = (type = json,
                   strip_outer_array = true);

select * from Stg_covid_transactions;

select
  states_json['death'],
  states_json['positive']
  from
    Stg_covid_transactions;

select states_json['date'] from Stg_covid_transactions;