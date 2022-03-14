create view relation as
select death/positive * 100 as rel
from Fact_Covid_transactions
where positive > 0
order by rel desc
limit 5
;

create view infection as
select state, (positive/(positive+negative)) * 100 as Infected
from Fact_Covid_transactions
where positive > 0 and negative > 0
order by Infected desc
limit 5;
