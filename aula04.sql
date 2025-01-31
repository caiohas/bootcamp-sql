-----------------------------------------------------------------------------------------------------------------------
-- Calcular utilizando GROUP BY:
-- Quais vendas eu tive?
-- Quantos produtos únicos dentro do mesmo pedido?
-- Quantidade de peças vendidas por pedido no total?
-- Qual é o valor total vendido por pedido?

SELECT 
	order_id
	,COUNT(DISTINCT product_id) AS total_unique_products
	,SUM(quantity) AS total_product_sales
	,SUM(unit_price * quantity) AS total_sales
FROM order_details
GROUP BY order_id
ORDER BY order_id

-----------------------------------------------------------------------------------------------------------------------
-- Calcular utilizando WINDOW FUNCTION:
-- Quais vendas eu tive?
-- Quantos produtos únicos dentro do mesmo pedido?
-- Quantidade de peças vendidas por pedido no total?
-- Qual é o valor total vendido por pedido?

SELECT 
	DISTINCT order_id
	,COUNT(product_id) OVER (PARTITION BY order_id) AS total_unique_products
	,SUM(quantity) OVER (PARTITION BY order_id) AS total_product_sales
	,SUM(unit_price * quantity) OVER (PARTITION BY order_id) AS total_sales
FROM order_details
ORDER BY order_id

-----------------------------------------------------------------------------------------------------------------------
-- Visao por linha do PARTITION BY
SELECT 
	order_id
	,product_id
	,unit_price
	,COUNT(product_id) OVER (PARTITION BY order_id) AS total_unique_products
	,SUM(quantity) OVER (PARTITION BY order_id) AS total_product_sales
	,SUM(unit_price * quantity) OVER (PARTITION BY order_id) AS total_sales
FROM order_details
ORDER BY order_id

-----------------------------------------------------------------------------------------------------------------------
-- Quais são os valores mínimo, máximo e médio de frete pago por cada cliente usando GROUP BY? (tabela orders)
-- EXPLAIN ANALYZE (comando para analisar a execução da query)
SELECT 
	customer_id
	,MIN(freight) AS frete_minimo
	,MAX(freight) AS frete_maximo
	,AVG(freight) AS frete_medio
FROM orders
GROUP BY customer_id
ORDER BY customer_id

-- Quais são os valores mínimo, máximo e médio de frete pago por cada cliente usando WINDOW FUNCTION? (tabela orders)
-- EXPLAIN ANALYZE (comando para analisar a execução da query)
SELECT 
	DISTINCT customer_id
	,MIN(freight) OVER (PARTITION BY customer_id) AS frete_minimo
	,MAX(freight) OVER (PARTITION BY customer_id) AS frete_maximo
	,AVG(freight) OVER (PARTITION BY customer_id) AS frete_medio
FROM orders
ORDER BY customer_id

-----------------------------------------------------------------------------------------------------------------------
-- ROW_NUMBER, RANK e DENSE_RANK

SELECT  
  o.order_id, 
  p.product_name, 
  (o.unit_price * o.quantity) AS total_sale,
  ROW_NUMBER() OVER (ORDER BY (o.unit_price * o.quantity) DESC) AS order_rn, -- atribui numero sequencial a cada linha
  RANK() OVER (ORDER BY (o.unit_price * o.quantity) DESC) AS order_rank, -- rankeia mas em caso de empate a proxima linha considera os empates
  DENSE_RANK() OVER (ORDER BY (o.unit_price * o.quantity) DESC) AS order_dense -- em caso de empate a proxima linha recebe a atribuicao sequencial
FROM  
  order_details o
JOIN 
  products p ON p.product_id = o.product_id

-----------------------------------------------------------------------------------------------------------------------
-- PERCENT_RANK e CUME_DIST
SELECT  
  order_id, 
  unit_price * quantity AS total_sale,
  ROUND(CAST(PERCENT_RANK() OVER (PARTITION BY order_id 
    ORDER BY (unit_price * quantity) DESC) AS numeric), 2) AS order_percent_rank, -- percentil
  ROUND(CAST(CUME_DIST() OVER (PARTITION BY order_id 
    ORDER BY (unit_price * quantity) DESC) AS numeric), 2) AS order_cume_dist -- distribuicao acumulada
FROM  
  order_details

-----------------------------------------------------------------------------------------------------------------------
-- LAG e LEAD
SELECT 
  customer_id, 
  TO_CHAR(order_date, 'YYYY-MM-DD') AS order_date, 
  shippers.company_name AS shipper_name, 
  LAG(freight) OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS previous_order_freight, -- registro anterior baseada no customer_id
  freight AS order_freight, 
  LEAD(freight) OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS next_order_freight -- registro subsequente baseada no customer_id
FROM 
  orders
JOIN 
  shippers ON shippers.shipper_id = orders.ship_via
