customer_table_drop= "DROP TABLE IF EXISTS dim"
order_table_dorp="DROP TABLE IF EXISTS orders"
product_table_drop="DROP TABLE IF EXISTS product"
ship_table_drop="DROP TABLE IF EXISTS ship"
sales_table_drop="DROP TABLE IF EXISTS sales"


customer_table_create = """CREATE TABLE IF NOT EXISTS customer(
                            id int not null,
                            customer_id VARCHAR, 
                            customer_name VARCHAR,
                            country VARCHAR ,
                            city VARCHAR ,
                            state VARCHAR ,
                            postalcode int,
                            region VARCHAR,
                            segment VARCHAR
                            
)"""

product_table_create = """CREATE TABLE IF NOT EXISTS product(
                        dimprodid int not null,
                        productid varchar,
                        category varchar,
                        subcategory varchar,
                        productname varchar
)
"""
order_table_create = """CREATE TABLE IF NOT EXISTS orders(
                        dimorderid int not null,
                        orderdate date,
                        month int,
                        year int,
                        day int
)
"""

ship_table_create = """CREATE TABLE IF NOT EXISTS ship(
                        dimshipidi int not null,
                        shipdate date,
                        month int,
                        year int,
                        day int 
)"""



sales_table_create = """CREATE TABLE IF NOT EXISTS sales(
                        rowid int not null,
                        orderid varchar,
                        dimorderid int,
                        dimshipid int,
                        dimcustomerid int,
                        dimproductid int,
                        sales decimal,
                        quantity int,
                        discount decimal,
                        profit int
)
"""
### Insert Queries
customer_table_insert=""" INSERT INTO public.customer(
	id, customer_id, customer_name, country, city, state, postalcode, region, segment)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s); """

order_table_insert=""" INSERT INTO orders (dimorderid,orderdate,month,year,day) values (%s,%s,%s,%s,%s)"""

ship_table_insert =""" INSERT INTO public.ship(
	dimshipidi, shipdate, month, year, day)
	VALUES (%s, %s, %s, %s, %s);""" 

product_table_insert = """ INSERT INTO product(dimprodid,productid,category,subcategory,productname) values (%s,%s,%s,%s,%s)"""
sales_table_insert = """ INSERT INTO public.sales(
	rowid, orderid, dimorderid, dimshipid, dimcustomerid, dimproductid, sales, quantity, discount, profit)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
### Query Lists

create_table_queries=[customer_table_create,order_table_create,ship_table_create,product_table_create,sales_table_create]
drop_table_queries=[customer_table_drop,order_table_dorp,ship_table_drop,product_table_drop,sales_table_drop]
