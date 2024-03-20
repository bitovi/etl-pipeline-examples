INSERT INTO products (product_id, product_name, price) VALUES
  (1, 'Cheeseburger', 1100),
  (2, 'Hot dog', 799),
  (3, 'French Fries', 399),
  (4, 'Chips', 199),
  (5, 'Fruit', 299),
  (6, 'Drink', 399);

do $$
begin
	for i in 1..10000 loop
		INSERT INTO orders (products, total)
		SELECT ARRAY(SELECT trunc(random() * 6 + 1)::INTEGER FROM generate_series(1, trunc(random() * 5 + 1)::INTEGER)), 0;
	end loop;
end; $$
