
--How many users were active on a given day (they made a deposit or withdrawal)
select count (distinct user_id) from public.fact_transactions 
where transaction_type IN ('Deposit', 'Withdrawal')--Currently there are only these two types of transactions in the transaction table, however the filter is included thinking about the possibility of including some other transaction in the future.
  AND transaction_date = '2022-05-20';--Replace date with the given day


--Identify users haven't made a deposit
SELECT count(distinct user_id)
FROM Dim_User
WHERE user_id NOT IN (
    SELECT DISTINCT user_id
    FROM Fact_Transactions
    WHERE transaction_type = 'Deposit' --Select all users with a deposit and filter users not included
);

--Average deposited amount for Level 2 users in the mx jurisdiction
select avg(amount) as avg_deposite --amount not rounded, in case you want to reduce the decimal size we can use round and cast
from Fact_Transactions a
inner join public.dim_userlevel b on a.user_id = b.user_id
left join public.dim_jurisdiction c on b.jurisdiction_id= c.jurisdiction_id
where a.transaction_type='Deposit' and b.level = 2  and c.country = 'mx';


--Latest user level for each user within each jurisdiction
select user_id, level, b.country as jurisdiction 
from (select user_id, level,
		jurisdiction_id, row_number() over (partition by user_id,jurisdiction_id order by effective_timestamp desc) rnum
		from public.dim_userlevel)a
left join public.dim_jurisdiction b on a.jurisdiction_id= b.jurisdiction_id
where a.rnum=1;	--select only the latest record base on effective_timestamp 


--Identify on a given day which users have made more than 5 deposits historically
SELECT user_id
FROM Fact_Transactions
WHERE transaction_type = 'Deposit' AND transaction_date <= '2022-05-20'  -- Replace with the specific date
GROUP BY user_id
HAVING COUNT(*) > 5;


--When was the last time a user made a login
SELECT user_id, MAX(event_date) AS last_login
FROM Fact_Events
WHERE event_type = 'login'
AND user_id= 'f62fc2d463b533cc7e121a3d5479b9b0' -- If you would like the query for a specific user if not just comment the line
GROUP BY user_id; 


--How many times a user has made a login between two dates
SELECT user_id, COUNT(*) AS login_count
FROM Fact_Events
WHERE event_type = 'login' AND event_date BETWEEN '2010-01-01' AND '2023-01-01'  -- Replace 'start_date' and 'end_date' with actual dates
AND user_id= 'f62fc2d463b533cc7e121a3d5479b9b0'-- If you would like the query for a specific user if not just comment the line
GROUP BY user_id;


--Number of unique currencies deposited on a given day
select transaction_date,count(distinct currency)unique_currencies
from public.fact_transactions 
where transaction_type = 'Deposit'
and transaction_date = '2022-05-20' -- Replace with the specific date
group by transaction_date;

--Number of unique currencies withdrew on a given day
select transaction_date,count(distinct currency)unique_currencies
from public.fact_transactions 
where transaction_type = 'Withdrawal'
and transaction_date = '2022-05-20'  -- Replace with the specific date
group by transaction_date;


--Total amount deposited of a given currency on a given day
SELECT transaction_date AS date, SUM(amount) AS total_deposit_amount
FROM Fact_Transactions
WHERE transaction_type = 'Deposit' AND currency = 'usd' AND transaction_date = '2022-05-20'  -- Replace 'given_currency' and 'given_date' as needed
GROUP BY transaction_date;
