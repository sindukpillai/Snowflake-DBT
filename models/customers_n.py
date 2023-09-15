import snowflake.snowpark.functions as F


def model(dbt, session):


   stg_customers_df = dbt.ref('stg_customers_n')
   stg_orders_df = dbt.ref('stg_orders_n')
   stg_payments_df = dbt.ref('stg_payments_n')


   customer_orders_df = (
       stg_orders_df
       .group_by('customer_id')
       .agg(
           F.min(F.col('orderdate')).alias('first_order'),
           F.max(F.col('orderdate')).alias('most_recent_order'),
           F.count(F.col('orderid')).alias('number_of_orders')
       )
   )


   customer_payments_df = (
       stg_payments_df
       .join(stg_orders_df, stg_payments_df.orderid == stg_orders_df.orderid, 'left')
       .group_by(stg_orders_df.customerid)
       .agg(
           F.sum(F.col('amount')).alias('total_amount')
       )
   )


   final_df = (
       stg_customers_df
       .join(customer_orders_df, stg_customers_df.customerid == customer_orders_df.customerid, 'left')
       .join(customer_payments_df, stg_customers_df.customerid == customer_payments_df.customerid, 'left')
       .select(stg_customers_df.customerid.alias('customer_id'),
               stg_customers_df.first_name.alias('first_name'),
               stg_customers_df.last_name.alias('last_name'),
               customer_orders_df.first_order.alias('first_order'),
               customer_orders_df.most_recent_order.alias('most_recent_order'),
               customer_orders_df.number_of_orders.alias('number_of_orders'),
               customer_payments_df.total_amount.alias('customer_lifetime_value')
       )
   )


   return final_df