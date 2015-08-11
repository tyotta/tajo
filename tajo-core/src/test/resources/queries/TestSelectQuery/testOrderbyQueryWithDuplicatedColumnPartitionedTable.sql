select c_nationkey, c_address, c_name, max(c_custkey) from customer_dup_parts
group by c_address, c_nationkey, c_name
order by c_address, c_nationkey, c_name;