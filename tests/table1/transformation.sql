with union_table as 
(select * from customer
union all 
select * from backup_customer_one),
rnk_table as
(select *,dense_rank() over (partition by customer_id order by created_at desc) rnk
from union_table)
select customer_id,first_name,last_name,email,created_at
from rnk_table
where rnk = 1

