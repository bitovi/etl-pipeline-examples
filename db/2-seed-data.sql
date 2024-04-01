insert into products (product_id, product_name, price) values
  (1, 'Cheeseburger', 1100),
  (2, 'Hot dog', 799),
  (3, 'French Fries', 399),
  (4, 'Chips', 199),
  (5, 'Fruit', 299),
  (6, 'Drink', 399);

do $$
begin
	for i in 1..10000 loop
		insert into orders (products, total)
		select array(select trunc(random() * 6 + 1)::integer from generate_series(1, trunc(random() * 5 + 1)::integer)), 0;
	end loop;
end; $$;

update orders as o
set total = (
     select sum(p.price)
     from products AS p
     where p.product_id = any(o.products)
)
where order_id % 7 != 0
