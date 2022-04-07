CREATE OR REPLACE DATABASE staging;

CREATE OR REPLACE TABLE Stg_covid_states(states_parquet variant);
desc table Stg_covid_states;
CREATE OR REPLACE STAGE parquet_stage url = 's3://staging-j/covid_states.parquet';


CREATE OR REPLACE PIPE loading_s3
    auto_ingest = true
    as
    copy into Stg_covid_states
    from @parquet_stage
    file_format = (type = 'parquet');

COPY INTO Stg_covid_states From @parquet_stage
    file_format = (type = parquet);


select states_parquet['name'] as name, states_parquet['state_code'] as state_code
from Stg_covid_states;