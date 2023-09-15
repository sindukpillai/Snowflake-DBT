with customers as (
   select * from {{ ref('stg_customers_n') }}
),
orders as (
   select * from {{ ref('stg_orders_n') }}
),
payments as (
   select * from {{ ref('stg_payments_n') }}
),
customer_orders as (
       select
       customerid,
       min(orderdate) as first_order,
       max(orderdate) as most_recent_order,
       count(orderid) as number_of_orders
   from orders
   group by customerid
),
customer_payments as (
   select
       orders.customerid,
       sum(amount) as total_amount
   from payments
   left join orders on
        payments.orderid = orders.orderid
   group by orders.customerid
),
final as (
   select
       customers.customerid,
       customers.firstname,
       customers.lastname,
       customer_orders.first_order,
       customer_orders.most_recent_order,
       customer_orders.number_of_orders,
       customer_payments.total_amount as customer_lifetime_value
   from customers
   left join customer_orders
       on customers.customerid = customer_orders.customerid
   left join customer_payments
       on  customers.customerid = customer_payments.customerid
)
select * from final