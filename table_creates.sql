/*STAGE*/

CREATE TABLE stage_bonus_prog.customers_extended_mirror (
	customer_id int8 ,
	"name" varchar(255) ,
	email varchar(255) ,
	phone_number varchar(255) ,
	created_at timestamp ,
	updated_at timestamp ,
	program_id int8 ,
	program_enrollment_date date ,
	program_name varchar(255) ,
	program_description text 
)
DISTRIBUTED BY (customer_id);

CREATE TABLE stage_bonus_prog.pointshistory_mirror (
	customer_id int8 ,
	"date" date ,
	points int8 ,
	transaction_type varchar(256) ,
	description text ,
	created_at timestamp ,
	updated_at timestamp 
)
DISTRIBUTED BY (customer_id, "date");


CREATE TABLE stage_bonus_prog.purchases_extended_mirror (
	purchase_id int8 ,
	customer_id int8 ,
	retailer_id int8 ,
	purchase_date date ,
	total_amount numeric(20, 2) ,
	points_earned int8 ,
	items varchar(256) ,
	created_at timestamp ,
	updated_at timestamp ,
	retail_name varchar(256) ,
	retail_location varchar(256) 
)
DISTRIBUTED BY (purchase_id);





/*STG*/
create table stg_bonus_prog.dim_customer(
   uk int8,
   first_name varchar(256),
   last_name  varchar(256),
   email  varchar(256),
   phone_number  varchar(256),
   program_uk int8,
   program_enrollment_date date,
   created_at timestamp,
   updated_at timestamp,
   as_of_day timestamp 
)
with(
    compresstype=zstd,
appendonly = true, 
compresslevel=7, 
orientation=row 
) distributed randomly
partition by range(program_enrollment_date) (
    START ('2020-01-01'::date) END ('2026-01-01'::date) EVERY ('1 month'::interval) 
);

create table stg_bonus_prog.fact_pointshistory(
customer_uk int8,
report_date date,
transaction_type varchar(256),
points int8,
as_of_day timestamp
)
with(
    compresstype=zstd,
appendonly = true, 
compresslevel=7, 
orientation=row 
) distributed randomly
partition by range(report_date) (
    START ('2020-01-01'::date) END ('2026-01-01'::date) EVERY ('1 month'::interval) 
);

create table stg_bonus_prog.fact_purchase(
    uk int8,
    retailer_uk int8,
    customer_uk int8,
    as_of_day timestamp,
    purchase_date timestamp,
    total_amount int8,
    points_earned int8,
    items varchar(256)
)
with (
      compresstype=zstd,
appendonly = true, 
compresslevel=7, 
orientation=row 
) distributed randomly
partition by range(purchase_date) (
    START ('2020-01-01'::date) END ('2026-01-01'::date) EVERY ('1 month'::interval) 
);

create table stg_bonus_prog.dim_program(
    uk int8, 
    program_name varchar(256),
    program_description varchar(256),
    as_of_day timestamp
)
with (  compresstype=zstd,
appendonly = true, 
compresslevel=7, 
orientation=row ) distributed randomly
partition by range(as_of_day) (
    START ('2020-01-01'::date) END ('2026-01-01'::date) EVERY ('1 month'::interval) 
);

create table stg_bonus_prog.dim_retailer(
uk int8,
name varchar(256),
location varchar(256),
as_of_day timestamp
)
with (  compresstype=zstd,
appendonly = true, 
compresslevel=7, 
orientation=row ) distributed randomly
partition by range(as_of_day) (
    START ('2020-01-01'::date) END ('2026-01-01'::date) EVERY ('1 month'::interval) 
);

/*DATAMART*/
create table dm_bonus_prog.dm_pointshistory_by_program(
    report_date date,
    points int8,
    transaction_type varchar(256),
    program_name varchar(256),
    as_of_day timestamp
)
with (  compresstype=zstd,
appendonly = true, 
compresslevel=7, 
orientation=row ) distributed randomly
partition by range(as_of_day) (
    START ('2020-01-01'::date) END ('2026-01-01'::date) EVERY ('1 month'::interval) 
);

create table dm_bonus_prog.dm_amounts_by_retailer(
name varchar(256),
purchase_date date,
total_amount int8,
as_of_day timestamp
)
with (
     compresstype=zstd,
appendonly = true, 
compresslevel=7, 
orientation=row 
) distributed randomly
partition by range(as_of_day) (
    START ('2020-01-01'::date) END ('2026-01-01'::date) EVERY ('1 month'::interval) 
);
