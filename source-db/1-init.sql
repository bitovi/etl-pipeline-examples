CREATE TABLE IF NOT EXISTS product (
  product_id SERIAL PRIMARY KEY,
  product_name varchar(45),
  price integer
);

CREATE TABLE IF NOT EXISTS "order" (
  order_id SERIAL PRIMARY KEY,
  products integer[],
  total integer
);
