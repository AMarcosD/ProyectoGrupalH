
import pandas as pd
import numpy as np
from datetime import datetime
import os
import shutil
from geopy.geocoders import Nominatim 
geolocator = Nominatim(user_agent="MakroAnalyse")
import psycopg2
from psycopg2 import Error
import psycopg2.extras  


database = 'db_olist'
user = 'Grupo5@olist-db-pg5'
password = 'Datascience22'
host = 'olist-db-pg5.postgres.database.azure.com'
port = '5432'


import psycopg2

try:  
    # connection establishment
    conn = psycopg2.connect(
    database= database,
        user= user,
        password= password,
        host= host,
        port= port
    )
    
    conn.autocommit = True
        
    # Creating a cursor object
    cursor = conn.cursor()
    
    # query to create a database 
    sql = ''' CREATE database dbt;'''
    # executing above query
    cursor.execute(sql)
    print("Database has been created successfully !!");
    
    # Closing the connection
    conn.close()
except (Exception, psycopg2.Error) as error:
            print(error)


database = 'dbt'

# CREACIÓN DE TABLAS


#CATEGORY_PRODUCT

try:
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE category_product(product_name_id integer, product_category_name varchar, product_category_name_english varchar, PRIMARY KEY (product_category_name_english));")
        conn.commit()
except (Exception, psycopg2.Error) as error:
        print(error)


#CLOSE_DEALS

try:
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE closed_deal(closed_id integer, mql_id varchar, seller_id varchar, won_date timestamp, business_segment varchar, PRIMARY KEY (closed_id));")
        conn.commit()
except (Exception, psycopg2.Error) as error:
        print(error)


#GEOLOCATION

try:
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user , password)
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE geolocation (zip_code integer, geolatitud float, geolongitud float, city varchar, state_name varchar, PRIMARY KEY (zip_code));")
        conn.commit()
except (Exception, psycopg2.Error) as error:
        print(error)


#PRODUCT

try:
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user , password)
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE product (product_id varchar, product_category_name_english varchar, product_weight_g float, product_length_cm float, product_height_cm float, product_width_cm float, PRIMARY KEY (product_id))")
        conn.commit()
except (Exception, psycopg2.Error) as error:
        print(error)


#PAYMENT

try:
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user , password)
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE payment(payment_id integer, order_id varchar,payment_sequential integer, payment_type varchar, payment_installments integer, payment_value float, PRIMARY KEY(payment_id));")
        conn.commit()
except (Exception, psycopg2.Error) as error:
        print(error)


#data_order 

try:
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user , password)
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE data_order (order_id varchar, customer_id varchar, order_status varchar, order_purchase_timestamp timestamp,\
                order_approved_at timestamp, order_delivered_carrier_date timestamp, order_delivered_customer_date timestamp,\
                    order_estimated_delivery_date timestamp, PRIMARY KEY (order_id));")
        conn.commit()
except (Exception, psycopg2.Error) as error:
        print(error)


#item

try:
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user , password)
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE item(orderitem_id integer, order_id varchar, product_id varchar, seller_id varchar, shipping_limit_date timestamp, quantity integer, price float, freight_value float, percentagePF float, PRIMARY KEY (orderitem_id))")
        conn.commit()
except (Exception, psycopg2.Error) as error:
        print(error)


#data_review

try:
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user , password)
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE data_review (review_id integer, order_id varchar, review_score integer, review_creation_date date, PRIMARY KEY (review_id));")
        conn.commit()
except (Exception, psycopg2.Error) as error:
        print(error)


#seller

try:
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user , password)
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE seller (seller_id varchar, zip_code int, city varchar, state_name varchar, PRIMARY KEY (seller_id));")
        conn.commit()
except (Exception, psycopg2.Error) as error:
        print(error)


#CUSTOMER

try:
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user , password)
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE customer (customer_id varchar, customer_unique_id varchar, zip_code integer, city varchar, state_name varchar, PRIMARY KEY (customer_id))")
        conn.commit()
except (Exception, psycopg2.Error) as error:
        print(error)


conn = psycopg2.connect(
database=database,
    user=user,
    password=password,
    host=host,
    port= port
)
conn.autocommit = True
cursor = conn.cursor()

cursor.execute("CREATE INDEX product_category_name_english ON category_product(product_category_name_english);")
cursor.execute("CREATE INDEX seller_id ON closed_deal(seller_id)")
cursor.execute('CREATE INDEX zip_code ON customer(zip_code)')
cursor.execute('CREATE INDEX order_id ON item(order_id)')
cursor.execute('CREATE INDEX product_id ON item(product_id)')
cursor.execute('CREATE INDEX item_seller_id ON item(seller_id)')
cursor.execute('CREATE INDEX payment_order_id ON payment(order_id)')
cursor.execute('CREATE INDEX product_product_category_name_english ON product(product_category_name_english)')
cursor.execute('CREATE INDEX review_order_id ON data_review(order_id)')
cursor.execute('CREATE INDEX seller_zip_code ON seller(zip_code)')
cursor.execute('CREATE INDEX customer_id ON data_order(customer_id)')

conn.close()

# VER!


conn = psycopg2.connect(
database= database,
    user= user,
    password=password,
    host=host,
    port= port
)
conn.autocommit = True
cursor = conn.cursor()

cursor.execute("ALTER TABLE product ADD CONSTRAINT category_product_fk_product FOREIGN KEY (product_category_name_english) REFERENCES category_product(product_category_name_english) ON DELETE NO ACTION ON UPDATE NO ACTION;")
cursor.execute("ALTER TABLE closed_deal ADD CONSTRAINT closed_deals_fk_seller FOREIGN KEY (seller_id) REFERENCES seller (seller_id) ON DELETE NO ACTION ON UPDATE NO ACTION;")
cursor.execute("ALTER TABLE customer ADD CONSTRAINT customer_fk_geolocation FOREIGN KEY (zip_code) REFERENCES geolocation (zip_code) ON DELETE NO ACTION ON UPDATE NO ACTION;")
cursor.execute("ALTER TABLE item ADD CONSTRAINT item_fk_seller FOREIGN KEY (seller_id) REFERENCES seller (seller_id) ON DELETE NO ACTION ON UPDATE NO ACTION;")
cursor.execute("ALTER TABLE item ADD CONSTRAINT item_fk_data_order FOREIGN KEY (order_id) REFERENCES data_order (order_id) ON DELETE NO ACTION ON UPDATE NO ACTION;")
cursor.execute("ALTER TABLE item ADD CONSTRAINT item_fk_product FOREIGN KEY (product_id) REFERENCES product (product_id) ON DELETE NO ACTION ON UPDATE NO ACTION;")
cursor.execute("ALTER TABLE payment ADD CONSTRAINT payment_fk_data_order FOREIGN KEY (order_id) REFERENCES data_order (order_id) ON DELETE NO ACTION ON UPDATE NO ACTION;")
cursor.execute("ALTER TABLE data_review ADD CONSTRAINT review_fk_data_order FOREIGN KEY (order_id) REFERENCES data_order (order_id) ON DELETE NO ACTION ON UPDATE NO ACTION;")
cursor.execute("ALTER TABLE seller ADD CONSTRAINT seller_fk_geolocation FOREIGN KEY (zip_code) REFERENCES geolocation (zip_code) ON DELETE NO ACTION ON UPDATE NO ACTION;")
cursor.execute("ALTER TABLE data_order ADD CONSTRAINT data_order_fk_customer FOREIGN KEY (customer_id) REFERENCES customer (customer_id) ON DELETE NO ACTION ON UPDATE NO ACTION;")


conn.close()


