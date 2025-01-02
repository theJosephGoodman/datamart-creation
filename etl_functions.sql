create or replace function init_dim_customer()
returns void as $$
begin 
	
truncate  stg_bonus_prog.dim_customer;
insert into stg_bonus_prog.dim_customer(uk, first_name,last_name,email, phone_number, program_uk, program_enrollment_date, created_at, updated_at,
as_of_day)
select customer_id as uk, split_part(trim(replace(replace(replace(replace(name, 'Ms.', ''), 'Mr.', ''), 'Dr.', ''), 'Mrs.', '')), ' ', 1) as first_name,
split_part(trim(replace(replace(replace(replace(name, 'Ms.', ''), 'Mr.', ''), 'Dr.', ''), 'Mrs.', '')), ' ', 2) as last_name,
email,
phone_number,
program_id as program_uk,
program_enrollment_date,
created_at, updated_at,
current_timestamp as as_of_day 
from (select customers_extended_mirror.*, row_number() over(partition by customer_id, program_id order by created_at desc) rn from stage_bonus_prog.customers_extended_mirror) cem where rn = 1;

	
end;
$$ language  plpgsql;

create or replace function increment_dim_customer()
returns void as $$
begin
	with source_table as (
select * from 
(
select customer_id as uk, split_part(trim(replace(replace(replace(replace(name, 'Ms.', ''), 'Mr.', ''), 'Dr.', ''), 'Mrs.', '')), ' ', 1) as first_name,
split_part(trim(replace(replace(replace(replace(name, 'Ms.', ''), 'Mr.', ''), 'Dr.', ''), 'Mrs.', '')), ' ', 2) as last_name,
email,
phone_number,
program_id as program_uk,
program_enrollment_date,
created_at, updated_at, row_number() over(partition by customer_id, program_enrollment_date order by created_at desc) rn
from stage_bonus_prog.customers_extended_mirror cem
where created_at >= current_timestamp - interval '14 day') a
where rn = 1
)
 -- обновляем строки
update stg_bonus_prog.dim_customer set
first_name =  source_table.first_name, 
last_name = source_table.last_name,
email =  source_table.email,
phone_number = source_table.phone_number,
program_enrollment_date =  source_table.program_enrollment_date ,
updated_at = source_table.updated_at ,
as_of_day = current_timestamp
from source_table
where dim_customer.uk = source_table.uk and dim_customer.program_uk = source_table.program_uk and dim_customer.program_enrollment_date = source_table.program_enrollment_date ;

	

	with source_table as (
select * from 
(
select customer_id as uk, split_part(trim(replace(replace(replace(replace(name, 'Ms.', ''), 'Mr.', ''), 'Dr.', ''), 'Mrs.', '')), ' ', 1) as first_name,
split_part(trim(replace(replace(replace(replace(name, 'Ms.', ''), 'Mr.', ''), 'Dr.', ''), 'Mrs.', '')), ' ', 2) as last_name,
email,
phone_number,
program_id as program_uk,
program_enrollment_date,
created_at, updated_at, row_number() over(partition by customer_id, program_enrollment_date order by created_at desc) rn
from stage_bonus_prog.customers_extended_mirror cem
where created_at >= current_timestamp - interval '14 day') a
where rn = 1
) 
--добавляем новые строки
insert into stg_bonus_prog.dim_customer(uk, first_name, last_name, email, phone_number, program_uk , program_enrollment_date, created_at, updated_at)
select uk, first_name, last_name, email, phone_number, program_uk , program_enrollment_date, created_at, updated_at
from source_table
where not exists (
select 1 from stg_bonus_prog.dim_customer
where dim_customer.uk = source_table.uk and dim_customer.program_uk = source_table.program_uk);
		
end;
$$ language  plpgsql;


create or replace function init_fact_pointshistory() returns void as $$
begin 
	truncate stg_bonus_prog.fact_pointshistory;
insert into stg_bonus_prog.fact_pointshistory(customer_uk, report_date, transaction_type, points, as_of_day )
select customer_id as customer_uk, date as report_date, transaction_type,sum(points), current_timestamp
from stage_bonus_prog.pointshistory_mirror
group by customer_id, date, transaction_type ;


end;
$$language plpgsql;

create or replace function increment_fact_pointshistory() returns void as $$
begin
	--обновление существующих строк
with source_table as (
select customer_id as customer_uk, date as report_date, transaction_type,sum(points) points, current_timestamp
from stage_bonus_prog.pointshistory_mirror
group by customer_id, date, transaction_type 
)
update stg_bonus_prog.fact_pointshistory
set points = source_table.points,
transaction_type = source_table.transaction_type,
as_of_day = current_timestamp
from source_table
where fact_pointshistory.report_date >=  (current_timestamp - interval '140 days')::date
and fact_pointshistory.customer_uk = source_table.customer_uk and fact_pointshistory.report_date = source_table.report_date
and (fact_pointshistory.points  <> source_table.points or fact_pointshistory.transaction_type <> source_table.transaction_type );
 

-- добавление новых строк
with source_table as (
select * from (
select customer_id as customer_uk, date as report_date, transaction_type,sum(points) points 
from stage_bonus_prog.pointshistory_mirror
group by customer_id, date, transaction_type 
) a
where report_date >= current_date - interval '1400 days'
)
insert into stg_bonus_prog.fact_pointshistory(customer_uk, report_date, transaction_type, points, as_of_day )
select customer_uk, report_date, transaction_type, points, current_timestamp
from source_table
where not exists (
select 1 from stg_bonus_prog.fact_pointshistory
where fact_pointshistory.customer_uk = source_table.customer_uk and fact_pointshistory.report_date = source_table.report_date);



	end;
$$language plpgsql;


create or replace function init_fact_purchase() returns void as $$
begin
truncate stg_bonus_prog.fact_purchase;
insert into stg_bonus_prog.fact_purchase(uk, retailer_uk, customer_uk, as_of_day, purchase_date, total_amount, points_earned, items)
select purchase_id as uk, retailer_id as retailer_uk, customer_id as customer_uk, current_timestamp as as_of_day, purchase_date, total_amount, points_earned, items 
from stage_bonus_prog.purchases_extended_mirror;
end;
$$language plpgsql;

create or replace function increment_fact_purchase() returns void as $$
begin
--обновление значений в строках 
	with source_table as (
select purchase_id as uk, retailer_id as retailer_uk, customer_id as customer_uk, current_timestamp as as_of_day, purchase_date, total_amount, points_earned, items 
from stage_bonus_prog.purchases_extended_mirror
where purchase_date >= current_date - interval '14 days'
)
update stg_bonus_prog.fact_purchase 
set
retailer_uk = source_table.retailer_uk,
customer_uk = source_table.customer_uk,
purchase_date = source_table.purchase_date,
total_amount = source_table.total_amount,
points_earned = source_table.points_earned,
items = source_table.items,
as_of_day = current_timestamp
from source_table
where fact_purchase.uk = source_table.uk and fact_purchase.as_of_day < source_table.as_of_day;

--вставка новых строк
	with source_table as (
select purchase_id as uk, retailer_id as retailer_uk, customer_id as customer_uk, purchase_date, total_amount, points_earned, items 
from stage_bonus_prog.purchases_extended_mirror
where uk > (select max(uk) from stg_bonus_prog.fact_purchase  )
)
insert into stg_bonus_prog.fact_purchase(
	uk ,
	retailer_uk,
	customer_uk ,
	as_of_day ,
	purchase_date ,
	total_amount ,
	points_earned ,
	items  )
select 	uk, retailer_uk,customer_uk ,
	current_timestamp ,
	purchase_date ,
	total_amount ,
	points_earned ,
	items 
from source_table;


end;
$$language plpgsql;


create or replace function init_dim_program() returns void as $$
begin
truncate stg_bonus_prog.dim_customer;

insert into stg_bonus_prog.dim_program(uk, program_name, program_description , as_of_day)
select  distinct program_id as uk, program_name, program_description, current_timestamp from stage_bonus_prog.customers_extended_mirror cem;


end;
$$language plpgsql;

create or replace function increment_dim_program() returns void as $$
begin
--обновление измененных строк
with source_table as (
select
	distinct program_id as uk,
	program_name,
	program_description
from
	stage_bonus_prog.customers_extended_mirror cem
where coalesce(updated_at, created_at) > (select max(as_of_day) from 	stg_bonus_prog.dim_program)
)

update
	stg_bonus_prog.dim_program
set
	program_name = source_table.program_name ,
	program_description = source_table.program_description,
	as_of_day = current_timestamp
from
	source_table
where
	dim_program.uk = source_table.uk;
--вставка новых строк
with source_table as (
select
	distinct program_id as uk,
	program_name,
	program_description,
	current_timestamp
from
	stage_bonus_prog.customers_extended_mirror cem

)
insert
	into
	stg_bonus_prog.dim_program(uk,
	program_name,
	program_description,
	as_of_day)
select
	uk, program_name, program_description, current_timestamp
from
	source_table
where
	uk > (
	select
		max(uk)
	from
		stg_bonus_prog.dim_program );
end;

$$language plpgsql;


create or replace function init_dim_retailer() returns void as $$
begin
truncate stg_bonus_prog.dim_retailer;

insert into stg_bonus_prog.dim_retailer(uk, name, location, as_of_day)
select distinct retailer_id as uk, retail_name, retail_location, current_timestamp from stage_bonus_prog.purchases_extended_mirror;


end;
$$language plpgsql;

create or replace function increment_dim_retailer() returns void as $$
begin
--обновление существующих строк
with source_table as (
select distinct retailer_id as uk, retail_name, retail_location, current_timestamp from stage_bonus_prog.purchases_extended_mirror
where created_at > current_timestamp - interval '14 days'
)
update stg_bonus_prog.dim_retailer
set
name = source_table.retail_name,
location= source_table.retail_location,
as_of_day = current_timestamp
from source_table
where dim_retailer.uk = source_table.uk
and (
name <> source_table.retail_name or 
location <> source_table.retail_location
);

--вставка новых строк в таблицу
insert into stg_bonus_prog.dim_retailer(uk, name, location, as_of_day)
select distinct retailer_id as uk, retail_name, retail_location, current_timestamp from stage_bonus_prog.purchases_extended_mirror
where retailer_id > (select max(uk) from stg_bonus_prog.dim_retailer);

end;
$$language plpgsql;



create or replace function dm_bonus_prog.init_dm_pointshistory_by_program() returns void as $$
begin
truncate dm_bonus_prog.dm_pointshistory_by_program;
insert into dm_bonus_prog.dm_pointshistory_by_program(report_date, points, transaction_type, program_name, as_of_day)
select report_date, points, transaction_type, program_name, current_timestamp from fact_pointshistory
inner join dim_customer
on fact_pointshistory.customer_uk = dim_customer.uk
inner join dim_program
on dim_customer.program_uk = dim_program.uk;
end;

$$language plpgsql;

create or replace function  dm_bonus_prog.increment_dm_pointshistory_by_program() returns void as $$
begin


--обновление существующих строк
with source_table as (
select distinct report_date, points, transaction_type, program_name from fact_pointshistory
inner join dim_customer
on fact_pointshistory.customer_uk = dim_customer.uk
inner join dim_program
on dim_customer.program_uk = dim_program.uk
where (fact_pointshistory.as_of_day > current_timestamp - interval '14 days') or (dim_customer.as_of_day > current_timestamp - interval '14 days') or (dim_program.as_of_day > current_timestamp - interval '14 days') 
) 
update  dm_bonus_prog.dm_pointshistory_by_program
set 
points = source_table.points
from source_table
where (dm_pointshistory_by_program.report_date = source_table.report_date and dm_pointshistory_by_program.transaction_type = source_table.transaction_type and dm_pointshistory_by_program.program_name = source_table.program_name) and dm_pointshistory_by_program.points <> source_table.points;


 --вставка новых строк
with source_table as (
select report_date, points, transaction_type, program_name from fact_pointshistory
inner join dim_customer
on fact_pointshistory.customer_uk = dim_customer.uk
inner join dim_program
on dim_customer.program_uk = dim_program.uk
where fact_pointshistory.as_of_day > (select max(as_of_day) from dm_bonus_prog.dm_pointshistory_by_program)
) 
insert into dm_bonus_prog.dm_pointshistory_by_program(report_date, points, transaction_type, program_name, as_of_day)
select report_date, points, transaction_type, program_name, current_timestamp from source_table;

end;
$$language plpgsql;
select dm_bonus_prog.increment_dm_pointshistory_by_program();

create or replace function dm_bonus_prog.init_dm_amounts_by_retailer() returns void as $$
begin
insert into dm_bonus_prog.dm_amounts_by_retailer(name, purchase_date, total_amount, as_of_day)
select name, purchase_date, total_amount, current_timestamp from stg_bonus_prog.fact_purchase
inner join dim_retailer
on fact_purchase.retailer_uk = dim_retailer.uk;

end;
$$language plpgsql;

