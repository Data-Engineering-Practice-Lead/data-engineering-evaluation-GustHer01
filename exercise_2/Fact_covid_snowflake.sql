CREATE OR REPLACE TABLE transactions(
    date string,
    state string,
    positive int,
    totalTestResults int,
    hospitalizedCumulative int,
    death int,
    hospitalized int,
    totalTestsViral int,
    positiveTestsViral int,
    negativeTestsViral int
);
CREATE OR REPLACE TABLE Fact_Covid_transactions(
    date string,
    state string,
    positive int,
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
        states_json['date'],
        states_json['state'],
        states_json['positive'],
        states_json['totalTestResults'],
        states_json['hospitalizedCumulative'],
        states_json['death'],
        states_json['hospitalized'],
        states_json['totalTestsViral'],
        states_json['positiveTestsViral'],
        states_json['negativeTestsViral']
        from
            Stg_covid_transactions;
CREATE OR REPLACE PROCEDURE dimensioning2()
    returns varchar
    language javascript
    as
    $$
    snowflake.execute({ sqlText:
        `
            merge into Fact_Covid_transactions fact
            using (select * from transactions) src
            on fact.state = src.state
            when matched and (fact.positive != src.positive or fact.totalTestResults != src.totalTestResults or fact.hospitalizedCumulative != src.hospitalizedCumulative
                               or fact.death != src.death or fact.hospitalized != src.hospitalized or fact.totalTestsViral!= src.totalTestsViral or fact.positiveTestsViral != src.positiveTestsViral
                               or fact.negativeTestsViral != src.negativeTestsViral)
            then
            update set fact.state = src.state
            when not matched
            then
            insert(date,state,positive , totalTestResults, hospitalizedCumulative,death,
                    hospitalized, totalTestsViral, positiveTestsViral, negativeTestsViral ) values(src.date, src.state,src.positive, src.totalTestResults, src.hospitalizedCumulative,src.death,
                                                                                                    src.hospitalized, src.totalTestsViral, src.positiveTestsViral, src.negativeTestsViral);

        `
    });
    return 'Done';
    $$
    ;
call dimensioning2();
