{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}
with orders as (
   select * from {{ ref('stg_orders_n') }}
),
payments as (
   select * from {{ ref('stg_payments_n') }}
),
order_payments as (
   select
       orderid,
       {% for payment_method in payment_methods -%}
       sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount,
       {% endfor -%}
       sum(amount) as total_amount
   from payments
   group by orderid
),
final as (
   select
       orders.orderid,
       orders.customerid,
       orders.orderdate,
       orders.status,
       {% for payment_method in payment_methods -%}
       order_payments.{{ payment_method }}_amount,
       {% endfor -%}
       order_payments.total_amount as amount
   from orders
   left join order_payments
       on orders.order_id = order_payments.orderid
)
select * from final