import pandas as pd
import psycopg2
from queries import *

def process_data(curr,filepath):
    df=pd.read_csv(filepath)

    df['order_date']=pd.to_datetime(df['order_date'])
    df['ship_date']=pd.to_datetime(df['ship_date']) 
    dimcustomer=df[['customer_id','customer_name','country','city','state','postalcode','region','segment']]
    dimcustomer=dimcustomer.drop_duplicates()
    dimcustomer['id']=range(1,len(dimcustomer)+1)
    dimcustomer=dimcustomer[['id','customer_id','customer_name','country','city','state','postalcode','region','segment']]
    for value in dimcustomer.values:
        id,customer_id,customer_name,country,city,state,postalcode,region,segment=value
        customer_data=(id,customer_id,customer_name,country,city,state,postalcode,region,segment)
      #  print(customer_table_insert)
       # print(customer_data)
        curr.execute(customer_table_insert,customer_data)

    dimorder=df[['order_date']]
    dimorder['oyear']=df['order_date'].dt.year
    dimorder['omonth']=df['order_date'].dt.month
    dimorder['oday']=df['order_date'].dt.day
    dimorder=dimorder.drop_duplicates()
    dimorder['dimorderid']=range(1,len(dimorder)+1)
    dimorder=dimorder[['dimorderid','order_date', 'oyear', 'omonth', 'oday']]
    for value in dimorder.values:
        dimorderid,order_date,oyear,omonth,oday=value
        order_data=(dimorderid,order_date,oyear,omonth,oday)
        curr.execute(order_table_insert,order_data)
        
    dimproduct=df[['product_id','category','sub_category','product_name']]
    dimproduct=dimproduct.drop_duplicates()
    dimproduct['dimproductid']=range(1,len(dimproduct)+1)
    dimproduct=dimproduct[['dimproductid','product_id','category','sub_category','product_name']]
    for value in dimproduct.values:
        dimproductid,product_id,category,sub_category,product_name=value
        product_data=(dimproductid,product_id,category,sub_category,product_name)
        
        curr.execute(product_table_insert,product_data)

    
    dimship=df[['ship_date']]
    dimship['smonth']=df['ship_date'].dt.month
    dimship['syear']=df['ship_date'].dt.year
    dimship['sday']=df['ship_date'].dt.day
    dimship=dimship.drop_duplicates()
    dimship['dimshipid']=range(1,len(dimship)+1)
    dimship=dimship[['dimshipid','ship_date', 'syear', 'smonth', 'sday']]
    for value in dimship.values:
        dimshipid,ship_Date,syear,smonth,sday=value
        ship_data=(dimshipid,ship_Date,syear,smonth,sday)
        curr.execute(ship_table_insert,ship_data)    
    
    factsales=df.merge(dimorder,on='order_date').merge(dimship,on='ship_date').merge(dimcustomer,on='customer_id').merge(dimproduct,on='product_id')[['row_id', 'order_id','dimorderid','dimshipid','id','dimproductid','sales','quantity', 'discount', 'profit']]
    factsales=factsales.drop_duplicates()
    for value in factsales.values:
        row_id,order_id,fdimorder_id,fdimshipid,fid,fdimproductid,sales,quantity,discount,profit=value
        sales_data=(row_id,order_id,fdimorder_id,fdimshipid,fid,fdimproductid,sales,quantity,discount,profit)
        curr.execute(sales_table_insert,sales_data)
  

def main():
    conn=psycopg2.connect(dbname="dmodeling",user="postgres",password=1234,host="localhost",port=5432)
    curr=conn.cursor()
    process_data(curr,filepath='/Users/sainathmorla/Downloads/sales1.csv')
    conn.commit()
    conn.close()

if  __name__=="__main__":

    main()