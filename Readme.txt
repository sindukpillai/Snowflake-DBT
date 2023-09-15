Dbt-snowflake-models 
Scenario:
Inventory dept will getting files in the forms of csv format and the size of the csv files are very less when compared to RDMS format of files for storage. 
Data manager is responsible to load these csv files in Amazon S3 storage with cheaper cost with maximum flexibilitty in order to process it when they want to generate some useful reports to find out best valuable customers or the improvement of sales for a particular time period,
ELT pipeines uses dbt tools for modeling along with snowflake as cloud data processing at the time of generating informations from raw data
ELT pipelines are creating diffrent models with the help of SQL files and python codes to generate statements to find out valuable customers and will store as tables or views storage for future uses.

So when they need various reports they take this intermediate result  tables and will hand over to the analytics dept or for PowerBI developers in order to create visualisations and there after for the critical business decisions.

Pros and Cons of ELT pipelines

Solutions will be scalable and distributed

Intermediate storage of models facilitate speedy accessing , computation and less fault tolerant systems

Developers, data engrs and data analysis teams will be more relaxed for their concerned areas of expertise while working 

Since files are stored as csv files only small amount of memory needed to store so the cost would be minimum for cloud environment

Enhancements of any area of the project will be easy as each of the areas is completely independent.

A model is created on this and is implemented successfully 

To create model 
==============================================
Install dbt
**********************************************
Prerequisites:
*********************************************
Pythin 3.10 or later  - install
Microsoft visual studio  - install
Install python and dbt plugins in VS
**********************************************

Go to terminal and change to dbt_workspace directory by CD
create python virtual env by 
python -m venv dbt_env

activate virtal environment by 
.\dbt_venv\Scripts\Activate.ps1
***********************************************
install dbt with data platform
pip install dbt-snowflake 

create config folder at %userprofile% with name .dbt
initialise dbt project with
init dbt
account name : dqhllyh-ox77163
ware house : COMPUTE_WH
Role : ACCOUNTADMIN
Schema:dbt_inventory
Database:INVENTORY
Username:xxxx
Password:

 take project in visual studio
take dbt_project.yml file under project directory
   

verify connection at vs terminal by
dbt debug


Create model in visual studio
************************************************
Table -customers

customerid integer
firstname varchar
lastname varchar
email varchar
address varchar
city varchar

Table- orders

orderid integer
orderdate date
customerid integer
emloyeeid integer
storeid integer
status varchar

**************CTE to find best customers*********************
************************************************************

with customerorders as (
   select c.customerid,concat(c.firstname,' ',c.lastname) as 
	customername,
   count(o.orderid) as nooforders from PUBLIC.CUSTOMERS c 
   join PUBLIC.ORDERS o on c.customerid=o.customerid 
   group by c.customerid,customername 
   order by nooforders desc)
   select customerid,customername,nooforders from customerorders

***************************************************************
*************************************************************
Create new file under model in vs project
customerorders.sql
paste the above SQL statement 
save file

terminal - to run model
dbt run
model is created and output is saved as view under inventory database
view format output can be changed to table for the benefit of data analysts 
by adding a statement in customerorders.sql model file created
{{ config(materialized='table')}}
and materialized='table' instead of 'view' in project.yml file

Then the outut is created as table under inventory database 
and the table can be 
imported directly to visualise in power BI

*****************************************************
model to find the customer revenue (quantity*unitprice)
****************************************************
with customerrevenue as (
   select c.customerid,concat(c.firstname,' ',c.lastname) as 
	customername,
   count(distinct o.orderid) as ordercount, sum(OI.Quantity*OI.Unitprice) as revenue 
   from PUBLIC.CUSTOMERS c 
   join PUBLIC.ORDERS o on c.customerid=o.customerid 
   join PUBLIC.ORDERITEMS OI on o.orderid=OI.orderid
   group by c.customerid,customername 
   order by revenue desc)
   select customerid,customername,revenue from customerrevenue 
*****************************************************
customerstage.sql
select
customerid,
firstname,
lastname,
email,
concat(firstname,'  ',lastname)as customername
from
PUBLIC.CUSTOMERS

Order_stage.sql
select
orderid,
orderdate,
customerid.
emloyeeid.
storeid,
status 
from 
PUBLIC.ORDERS

orderitems_stg.sql
select
orderitemid,
orderid,
productid,
quantity,
unitprice,
quantity*unitprice as totalprice
from
PUBLIC.ORDERITEMS

orders_fact.sql
select
o.orderid,
o.orderdate,
o.customerid,
o.emloyeeid,
o.storeid,
count(distinct o.orderid) as ordercount,
sum(OI.totalprice) as revenue
from
{{ref('order_stg')}} o
join
{{ref('orderitems_stg')}} OI on o.orderid=OI.orderid
group by
o.orderid,
o.orderdate,
o.customerid,
o.emloyeeid,
o.storeid

customerrevenue1.sql
select 
OS.customerid,
c.customername,
sum(OS.ordercount) as ordercount,
sum(OS.revenue) as revenue
from
{{ref('orders_fact')}} OS
join
{{ref('customers_stg')}} c on OS.customerid=c.customerid
group by
OS.customerid,
c.customername

dbt_project.yml
models:
	oms_dbt_proj:
		customers_stg:
			schema:PUBLIC
		orders_stg:
			schema:PUBLIC
		orderitems_stg:
			schema:PUBLIC
		customerrevenue1:
			materialized:table
		orders_fact:
			materialized:table
			schema:PUBLIC	

{% macro generate_schema_name(custom_schema_name, node)-%}
	{%- set default_schema=target.schema -%}
	{%- if custom_schema_name is none -%}
		{{default_schema}}
	{%- else -%}
		{{custom_schema_name | trim}}
	{%- endif -%}
{%- endmacro %}


{{ config(materialized='table')}}

with customerrevenue as (
   select c.customerid,concat(c.firstname,' ',c.lastname) as 
	customername,
   count(distinct o.orderid) as ordercount, sum(OI.Quantity*OI.Unitprice) as revenue 
   from PUBLIC.CUSTOMERS c 
   join PUBLIC.ORDERS o on c.customerid=o.customerid 
   join PUBLIC.ORDERITEMS OI on o.orderid=OI.orderid
   group by c.customerid,customername 
   order by revenue desc)
   select customerid,customername,revenue from customerrevenue 

#example:
      #+materialized: table
88888888888888888888888888888888888888888888888'o
python -sql grouping 
https://towardsdatascience.com/data-grouping-in-python-d64f1203f8d3
https://mode.com/sql-tutorial/sql-window-functions/
https://stackoverflow.com/questions/51631096/python-pandas-aggregation-with-condition
https://www.youtube.com/watch?v=BZCIUooQw6g
8888888888888888888888888888888888888888888888

import snowflake.snowpark.functions as f
from snowflake.snowpark.functions import col

def model(dbt,session):
	dbt.config(materialized='table')
	df_cust=dbt.ref("stg_customers")
	df_ord=dbt.ref("stg_orders")
	df_pay=dbt.ref("stg_payments")

	df_customer_orders=df_ord.group by(col("customerid").agg(f.min(col("order_date")).alias("first_order"),f
	df_customer_payment=df_pay.jon(df_ord,df_ord.orderid==df_pay.order_id,join_type="left").select(df_ord
	df_final=df_cust.join(df_customer_orders,df_customer_orders.customerid==df_customerid,join_type=
	return df_final

import snowflake.snowpark.functions as f
from snowflake.snowpark.functions import col,iff,lit
from functions import reduce
from decimal import *

payment_methods=['credit_card','coupon','bank_transfer','gift_card']

def model(dbt.session):
	dbt.config(materialized='table')
	df_ord=dbt.ref("stg_orders")
	df_pay=dbt.ref("stg_payment")
	
	df_payment_types=df_pay.drop("payment_id").pivot("payment_method",payment_methods).sum('amount').na.fill(decimal
	df_renamed.renameColumns(df_payment_types)
	df_order_payments=df_pay.group_by(col("order-id").agg(f.sum(col("amont")).alias("total_amount")))

	df_new=df_order_payments.join(df_renamed,df_renamed_order_id==df_order_payments.order_id).drop(df_order_payments
	df_final=df_ord.join(dfnew,df_ord.orderid==df_new.order_id,join_type="left").drop(df_new.order_id).with
	return df_final

def rename(columns(df)):
	colscleared-[col[2:-2] if col.startswith('"') and col.endswith('"') else col for col in df.columns]
	df_cleared=reduce(lambda(df,0

***************************************************
To integrate snowflake with Amazon S3 bucket 
do the following
*******************************************
Login to aws console account
Create S3 bucket with name dbtproj
choose region as North Virginea (cheaper)
click on create bucket 

S3-> Simple Storage Service are cloud storage used to store files

Select the newly created bucket in aws
create a new folder and upload csv files into that folder
Close S3 bucket
Goto IAM role in aws and create a new role
Select amazons3fullacess policy
set role name as dbtproj and create the role

Take  the newly create role and copy ARN (Amazon Resource Name)
to be pasted in snowflake for integration
*************************************
Login/Create snowflake free trial account and go to account
Create storage integration following code

create or replace storage integration dbtproj_int
TYPE=EXTERNAL_STAGE
STORAGE_PROVIDER=S3
ENABLED=TRUE
STORAGE_AWS_ROLE_ARN='arn:aws:iam::705506267817:role/dbtproj' (ARN copied from AWS IAM role account trusted policy)

STORAGE_ALLOWED_LOCATIONS='s3://dbtproj/csv files/'
COMMENT='int with amazon s3 bucket'

desc integration dbtproj_int;

create or replace STAGE PUBLIC.aws_stage_aws
URL='S3://dbtproj/csv files/'
STORAGE_INTEGRATION=dbtproj_int ;

list@aws_stage_aws

After executing this files stored in S3 bucket will be loaded by snowflake and will create tables with that values in snowflake database

****************************************************
