CREATE OR REPLACE TABLE transactions(
    dates date,
    state string,
    positive int,
    negative int,
    totalTestResults int,
    hospitalizedCumulative int,
    death int,
    hospitalized int,
    totalTestsViral int,
    positiveTestsViral int,
    negativeTestsViral int,
    state_id int references covid_states(id_state),
    date_id int references calendar(id_date)
);
CREATE OR REPLACE TABLE Fact_Covid_transactions(
    dates date,
    state string,
    positive int,
    negative int,
    totalTestResults int,
    hospitalizedCumulative int,
    death int,
    hospitalized int,
    totalTestsViral int,
    positiveTestsViral int,
    negativeTestsViral int
);


insert into transactions
    select
        t.states_json['date'],
        t.states_json['state'],
        t.states_json['positive'],
        t.states_json['negative'],
        t.states_json['totalTestResults'],
        t.states_json['hospitalizedCumulative'],
        t.states_json['death'],
        t.states_json['hospitalized'],
        t.states_json['totalTestsViral'],
        t.states_json['positiveTestsViral'],
        t.states_json['negativeTestsViral'],
        c.id_state,
        cal.id_date
        from
            Stg_covid_transactions as t
        join covid_states as c
            on t.states_json['state'] = c.state_code
        join calendar as cal
            on t.states_json['date'] = cal.date;

select * from transactions;
