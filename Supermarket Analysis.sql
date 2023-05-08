SELECT EXTRACT (ISODOW FROM date) AS day_of_the_week
FROM sales

CREATE TABLE sales (
	invoice_id VARCHAR(20) NOT NULL PRIMARY KEY,
	branch VARCHAR(2) NOT NULL,
	city VARCHAR(20) NOT NULL,
	customer_type VARCHAR(20) NOT NULL,
	gender VARCHAR(10) NOT NULL,
	product_line VARCHAR(50) NOT NULL,
	unit_price NUMERIC NOT NULL,
	quantity INTEGER NOT NULL,
	tax_5 NUMERIC NOT NULL,
	total NUMERIC NOT NULL,
	date DATE  NOT NULL,
	time TIME NOT NULL,
	payment VARCHAR(50) NOT NULL,
	cogs NUMERIC NOT NULL,
	gross_margin_percentage NUMERIC NOT NULL,
	gross_income NUMERIC NOT NULL,
	rating NUMERIC NOT NULL
)

								---- Data Cleaning
SELECT *
FROM sales;

-- Checking for duplicates
SELECT COUNT(*)
FROM sales;

SELECT COUNT(DISTINCT invoice_id) 
FROM sales

SELECT *
FROM sales
LIMIT 5;

SELECT SUM(unit_price)
FROM sales

SELECT SUM(cogs)
FROM sales

SELECT SUM(total)
FROM sales

-- Changing incorrect spelling (Naypyitaw to Naypyidaw)
UPDATE sales
SET city = REPLACE(city, 'Naypyitaw', 'Naypyidaw');

-- Add a new column that contains the extracted months (Jan, Feb, Mar)
SELECT to_char(date, 'Month') AS month
FROM sales

ALTER TABLE sales
ADD COLUMN Month VARCHAR(50);

UPDATE sales
SET Month = to_char(date, 'Month');

-- Add a new column that contains the extracted Weekdays (Mon, Tue, Wed, Thur, Fri, Sat and Sun)
SELECT to_char(date, 'Day') AS weekdays
FROM sales

ALTER TABLE sales
ADD COLUMN weekday VARCHAR(50);

UPDATE sales
SET weekday = to_char(date, 'Day');

-- Add a new column named day_period to give insight of sales in the Morning, Afternoon and Evening
SELECT *, (CASE WHEN time BETWEEN '6:00' AND '12:00'
		THEN 'Morning'
   		WHEN time BETWEEN '12:01' AND '16:00'
  		THEN 'Afternoon'
  		ELSE 'Evening'
  END) AS day_period
FROM sales;

ALTER TABLE sales
ADD COLUMN day_period VARCHAR(50);

UPDATE sales
SET day_period = (CASE WHEN time BETWEEN '6:00' AND '12:00'
		THEN 'Morning'
   		WHEN time BETWEEN '12:01' AND '16:00'
  		THEN 'Afternoon'
  		ELSE 'Evening'
  END);

-- Checking for null values in the dataset
SELECT *
FROM sales
WHERE invoice_id IS NULL 
	AND total IS NULL
	AND time IS NULL
	AND date IS NULL
	AND gross_income IS NULL;

-- Max, Min, Avg and total sales in the last 3 months 
SELECT MAX(total) AS "maximum_sales ($)", 
	ROUND(MIN(total), 2) AS "mininum_sales ($)", 
	ROUND(AVG(total), 2) AS "Average_sales ($)",
	ROUND(SUM(total), 0) AS "total_sales ($)"
FROM sales;

-- How many quantities of product do we sell in the last 3 months
SELECT SUM(quantity)
FROM sales;

-- Quantity of product sold in Jan, Feb, Mar
SELECT to_char(date, 'Month') AS Month,
	SUM(quantity) AS quantity_sold
FROM sales
GROUP BY Month, to_char(date, 'Month')
ORDER BY quantity_sold DESC;

										--- SALES ANALYSIS ----
-- When does the supermarket opens and closes for daily sales?
SELECT MIN(time) AS opening_time, 
	MAX(time) AS closing_time
FROM sales;

-- Total revenue in the last 3 months (Jan, Feb, Mar)
SELECT ROUND(SUM(total), 2) AS "total_revenue ($)"
FROM sales;

-- Total gross income in the last 3 months (Jan, Feb, Mar)
SELECT ROUND(SUM(gross_income), 2) AS "total_income ($)"
FROM sales;

-- Total number of orders received in the last 3 months (Jan, Feb, Mar)
SELECT COUNT(invoice_id)
FROM sales;

-- In which month have we achieved the highest and lowest revenue(total revenue in each month)?
SELECT to_char(date, 'Month') AS month,
	ROUND(SUM(total), 2) AS "total_revenue ($)"	
FROM sales
GROUP BY month, to_char(date, 'Month')
ORDER BY "total_revenue ($)" DESC;

-- Which day of the week have we achieved the highest and lowest sales?
SELECT to_char(date, 'DAY') AS weekdays,
	ROUND(SUM(total), 2) AS average_sales
FROM sales
GROUP BY weekdays
ORDER BY average_sales DESC;

-- Which period of the day has the highest sales?
SELECT (CASE
    WHEN time >= '06:00:00' AND time < '12:00:00' THEN 'morning'
    WHEN time >= '12:00:00' AND time < '16:00:00' THEN 'afternoon'
    ELSE 'evening'
END) AS period
	, SUM(total) AS total_sales
	, SUM(quantity) AS total_quantity
	, SUM(gross_income) AS total_income
FROM sales
GROUP BY period
ORDER BY total_sales DESC;

-- What is the total revenue generated by each customer type
SELECT customer_type, 
	ROUND(SUM(total), 2) AS revenue
FROM sales
GROUP BY customer_type;

-- What is the total quantity sold and revenue generated by each product line?
SELECT Product_line, 
	SUM(quantity) AS total_quantity, 
	SUM(total) AS total_revenue
FROM sales
GROUP BY Product_line
ORDER BY total_revenue DESC;

-- What is the total revenue for each product line?
SELECT product_line,
	SUM(total) AS total_sales
FROM sales
GROUP BY product_line
ORDER BY total_sales DESC;

-- What is the total gross income for each product line?
SELECT product_line, 
		SUM(gross_income) AS total_income
FROM sales
GROUP BY product_line
ORDER BY total_income DESC;

-- Which product has the highest and lowest average sales
SELECT product_line,
	ROUND(AVG(total), 2) AS average_sales
FROM sales
GROUP BY product_line
ORDER BY average_sales DESC;

--Find city whose sales were better than the average sales across all cities
SELECT *
FROM (SELECT city, sum(total) AS total_sales
	FROM sales
	GROUP BY city) sales
JOIN (SELECT AVG(total_sales) sales
	FROM (SELECT city, sum(total) AS total_sales
		FROM sales
		GROUP BY city) x) avg_sales
	ON sales.total_sales > avg_sales.sales;

with sales as 
	(SELECT city, sum(total) AS total_sales
	FROM sales
	GROUP BY city)
SELECT *
FROM sales
JOIN (SELECT AVG(total_sales) AS sales
	FROM sales x) avg_sales
	ON sales.total_sales > avg_sales.sales;

                         -- CUSTOMER ANALYSIS
						 
-- Which gender is our target market?
SELECT gender, 
	COUNT(*)
FROM sales
GROUP BY gender;

-- Average rating (customer experience)
SELECT ROUND(AVG(rating), 0)
FROM sales

-- What is the average customer rating for each city?
SELECT city, 
	ROUND(AVG(rating), 1) AS rating 
FROM sales 
GROUP BY city
ORDER BY rating DESC;

-- Which product line has the best and worst customer rating?
SELECT product_line, 
	ROUND(AVG(rating), 1) AS rating
FROM sales
GROUP BY product_line
ORDER BY rating DESC;

-- What is the customer experience for each branch?
SELECT branch, 
	ROUND(AVG(rating), 1) AS rating
FROM sales
GROUP BY branch
ORDER BY rating DESC;

-- Most customer used payment method for transaction
SELECT payment, COUNT(*)
FROM sales
GROUP BY payment
ORDER BY count DESC;

-- Which branch is generating the highest and lowest income in the last 3 months?
SELECT branch, 
	SUM(gross_income) AS total_income
FROM sales
GROUP BY branch
ORDER BY total_income DESC;

-- What is the total revenue generated by each branch?
SELECT branch, 
	SUM(total) AS "total_revenue ($)"
FROM sales
GROUP BY branch;

-- Which customer type buy more?
SELECT customer_type, COUNT(*)
FROM sales
GROUP BY customer_type;

-- What is the average rating for each gender?
SELECT gender, ROUND(AVG(rating), 0) AS avg_rating
FROM sales
GROUP BY gender;

-- What is the most common payment method for each gender
SELECT payment
    , COALESCE (male, 0) AS male
    , COALESCE (female, 0) AS female
FROM CROSSTAB ('SELECT payment
    			, gender
    			, count(gender) as total_gender
    			FROM sales
    			WHERE gender <> ''NA''
    			GROUP BY payment, gender
    			ORDER BY payment, gender',
            'values (''Male''), (''Female'')')
    AS FINAL_RESULT(payment varchar, male bigint, female bigint)
    ORDER BY male DESC, female DESC;

-- Which city is generating the highest revenue
SELECT city, 
	ROUND(SUM(total), 2) AS total_revenue
FROM sales
GROUP BY city
ORDER BY total_revenue DESC;

SELECT city, AVG(total) AS total_revenue
FROM sales
GROUP BY city
ORDER BY total_revenue DESC;

                                         --- PRODUCT ANALYSIS
						
-- What is the total revenue generated by each product line?
SELECT Product_line,
	SUM(total) AS revenue
FROM sales
GROUP BY Product_line
ORDER BY revenue DESC;

-- Quantity of goods sold in each product line
SELECT product_line, 
	SUM(quantity) AS quantity,
	SUM(cogs) AS cogs,
	SUM(total) AS total_sales
FROM sales
GROUP BY product_line
ORDER BY quantity DESC;

-- What is the most common product_line of each gender/sex (What are the interests of each gender)
CREATE extension tablefunc;

SELECT product_line
    , COALESCE (male, 0) AS male
    , COALESCE(female, 0) AS female
FROM CROSSTAB ('SELECT product_line
    			, gender
    			, count(gender) as total_gender
    			FROM sales
    			WHERE gender <> ''NA''
    			GROUP BY product_line, gender
    			ORDER BY product_line, gender',
            'values (''Male''), (''Female'')')
    AS FINAL_RESULT(product_line varchar, male bigint, female bigint)
    ORDER BY male DESC, female DESC;
	
-- Fetch all product line details and add remarks to those product line higher than average sales
SELECT *
, (CASE when total > (SELECT AVG(total) FROM sales)
  		then 'Higher than average'
  		else 'Lower than average'
  END) AS remarks
FROM sales;

OR

SELECT *
, (CASE when total > avg_sal.sal
  		then 'Higher than average'
  		else 'Lower than average'
  END) AS remarks
FROM sales
CROSS JOIN (SELECT AVG(total) sal FROM sales) avg_sal;

-- How many products were sold in each branch?
SELECT branch, 
	SUM(quantity) AS quantity
FROM sales 
GROUP BY branch;

-- HOW MANY PRODUCTS WERE SOLD IN EACH CITY?
SELECT city, 
	SUM(quantity) AS quantity
FROM sales 
GROUP BY city
ORDER BY quantity DESC;

-- Count the number of sales by customer type and gender
SELECT customer_type, gender, COUNT(*) AS sales_count 
FROM sales
GROUP BY customer_type, gender;

SELECT product_line, SUM(gross_income) AS avg_gross_income 
FROM sales
GROUP BY product_line
ORDER BY avg_gross_income DESC;
	
-- Find the branch(es) who have sold more products than the average units sold by all branches
SELECT branch, sum(quantity) AS quantity
FROM sales
GROUP BY branch
HAVING sum(quantity) > (SELECT AVG(quantity) FROM sales); 