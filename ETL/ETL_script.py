#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
from datetime import datetime
import os
import shutil
import mysql.connector as msql
from mysql.connector import Error
from geopy.geocoders import Nominatim 
geolocator = Nominatim(user_agent="MakroAnalyse")


# In[2]:


print('Introduzca la dirección donde se encuentren los conjuntos de datos: ')

direccion = input()
carpeta_datos = direccion + '\\'
lista_datos = []
for file in os.listdir(carpeta_datos):
    if file.endswith('.csv'):
        lista_datos.append(file)
    if file.endswith('.json'):
        lista_datos.append(file)
lista_datos = sorted(lista_datos)


# In[281]:


ruta1 = (carpeta_datos + 'product_category_name_translation.csv')
ruta2 = (carpeta_datos + 'olist_closed_deals_dataset.csv')
ruta3 = (carpeta_datos + 'geolocation_dataset_brazil.csv')
ruta4 = (carpeta_datos + 'olist_products_dataset.csv')
ruta5 = (carpeta_datos + 'olist_order_payments_dataset.csv')
ruta6 = (carpeta_datos +  'olist_orders_dataset.csv')
ruta7 = (carpeta_datos + 'olist_order_reviews_dataset.csv')
ruta8 = (carpeta_datos + 'olist_order_items_dataset.csv')
ruta9 = (carpeta_datos + 'olist_sellers_dataset.csv')
ruta10 = (carpeta_datos + 'olist_customers_dataset.csv')
rutaestados = (carpeta_datos + 'estados.json')


# In[164]:


print('Por favor, ingrese su usuario de MySQL: ')
user = input()
print('Por favor, ingrese su contraseña de MySQL: ')
password = input()
print('Por favor, ingrese el host de MySQL: ')
host = input()
database = 'db_olist'


# In[88]:


# CONEXIÓN AZURE
# conn = msql.connect(host='olist-henrygrupo5.mysql.database.azure.com', user= 'mnp_henry_grupo5@olist-henrygrupo5',
#                        password='Datascience22#')


# In[166]:


try:
    conn = msql.connect(host= host, user = user,
                        password = password)
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS db_olist")
        print("Database is created")
except Error as e:
    print("Error while connecting to MySQL", e)


# In[167]:


def etl_categoryproduct(ruta1):

    #Normalizacion
    data_category_product = pd.read_csv(ruta1)
    data_category_product.loc[71] = ['sin_categoria','NoCategory']
    data_category_product.reset_index(inplace=True)
    data_category_product.rename(columns={'index':'idproduct_name'}, inplace=True)


    #Carga a SQL
    try:
        conn = msql.connect(host= host, database= database, user= user, password= password)
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            cursor.execute('DROP TABLE IF EXISTS category_product;')
            cursor.execute("CREATE TABLE category_product(idproduct_name INT(11), product_category_name VARCHAR(255), product_category_name_english VARCHAR(255), PRIMARY KEY (idproduct_name))")
            for i,row in data_category_product.iterrows():

#OJOTAAAA QUE ACÀ HAY QUE CAMBIAR EL NOMBRE DE LA BASE DE DATOS CADA VEZ QUE HAGO UN INSERT EN TODAS LAS FUNCIONES
                sql = "INSERT INTO category_product VALUES (%s, %s, %s)"
                cursor.execute(sql, tuple(row))

                conn.commit()

    except Error as e:
                print("Error de conexión con MySQL", e)

    return data_category_product


# In[168]:


def etl_closed(ruta2):
    data_closed = pd.read_csv(ruta2)
    data_closed['won_date'] = pd.to_datetime(data_closed['won_date'])
    data_closed.drop(columns=['declared_monthly_revenue', 'sr_id', 'sdr_id', 'lead_behaviour_profile', \
        'has_company','lead_type','has_gtin','business_type' ,'average_stock', 'declared_product_catalog_size'], inplace=True)
    data_closed.reset_index(inplace=True)
    data_closed.rename(columns={'index':'closed_id'}, inplace=True)
    data_closed.dropna(inplace=True)

    

    try:
        conn = msql.connect(host= host, database= database, user= user, password= password)
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            
            record = cursor.fetchone()
            cursor.execute('DROP TABLE IF EXISTS closed_deals;')
           
            cursor.execute("CREATE TABLE closed_deals(closed_id INT(11), mql_id VARCHAR(255), seller_id VARCHAR(255), won_date DATETIME(0), business_segment VARCHAR(50), PRIMARY KEY (closed_id))")
            for i,row in data_closed.iterrows():
                #here %S means string values
                # Habria que chequear aca si funciona con los Ints o los Floats o Fechas,inclusive. Siempre creo que se usa el %s.
                sql = "INSERT INTO closed_deals VALUES (%s,%s,%s,%s,%s)"
                cursor.execute(sql, tuple(row))
                # the connection is not auto committed by default, so we must commit to save our changes
                conn.commit()
            


    except Error as e:
                print("Error de conexión con MySQL", e)    


# In[179]:


def etl_geolocalizacion(ruta3, rutaestados):
    geo = pd.read_csv(ruta3)
    geo.drop_duplicates(inplace=True)
    geo = geo.rename(columns={'geolocation_zip_code_prefix': 'zip_code', 'geolocation_lat':'geolatitud', \
        'geolocation_lng':'geolongitud', 'geolocation_city':'city', 'geolocation_state' : 'state'})
    dfj = pd.read_json(rutaestados)
    #Mergeamos ambas tablas
    geo = geo.merge(dfj[['nome', 'sigla']], right_on='sigla', left_on='state')

    #Renombramos columnas para unificar descripcion de mismo contenido de columna
    geo.rename(columns={'nome':'state_name'}, inplace=True)

    #Normalizamos mayúsculas
    geo['city'] = geo['city'].str.title()

    #Eliminamos duplicados por 'zip_code' 
    geo = geo.drop_duplicates('zip_code')

    #Reseteamos indice
    geo = geo.reset_index(drop=True)

    #Finalmente, nos quedamos con las columnas que nos servirán a futuro.
    geo = geo[['zip_code', 'geolatitud', 'geolongitud', 'city', 'state_name']]

    #Reemplazamos aquellas latitudes y longitudes que estén en positivo

    geo.loc[geo.geolongitud > 0, 'geolongitud'] = geo.geolongitud * (-1)
    geo.loc[geo.geolatitud > 5, 'geolatitud'] = geo.geolatitud * (-1)


    #'CREAMOS MÁSCARA DONDE SE CUMPLEN LAS 3 CONDICIONES'
    a = (geo.geolongitud < -73) | (geo.geolongitud > -34) | (geo.geolatitud < -33)

    #'Para buscar las coordenadas con más precisión'
    geo['full_data'] = geo.state_name + ',' + geo.city 

    #'Traemos las coordenadas con Geopy donde se cumple la máscara'

    geo.loc[a, 'gcode'] = geo[a]['full_data'].apply(geolocator.geocode)
    geo.loc[a,'lat'] = [g.latitude for g in geo[a]['gcode']]
    geo.loc[a,'long'] = [g.longitude for g in geo[a]['gcode']]
    
    #'Reemplazamos en columna original'

    geo.loc[a, 'geolatitud'] = geo[a]['lat']
    geo.loc[a, 'geolongitud'] = geo[a]['long']
    
    # Eliminamos columnas auxiliares
    geo.drop(columns= ['gcode', 'lat', 'long', 'full_data'], inplace=True)



    geo['zip_code'] = geo['zip_code'].astype(str)

    try:
        conn = msql.connect(host= host, database= database, user= user, password= password)
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            cursor.execute("DROP TABLE IF EXISTS geolocation;")
            cursor.execute("CREATE TABLE geolocation (zip_code INT(11), geolatitud DECIMAL(13,10), geolongitud DECIMAL(13,10), city VARCHAR (255), state_name VARCHAR (255), PRIMARY KEY (zip_code))")
            
                
            for i,row in geo.iterrows():
                #here %S means string values
                # Habria que chequear aca si funciona con los Ints o los Floats o Fechas,inclusive. Siempre creo que se usa el %s.
                sql = "INSERT INTO geolocation VALUES (%s,%s,%s,%s,%s)"
                cursor.execute(sql, tuple(row))
                # the connection is not auto committed by default, so we must commit to save our changes
                conn.commit()

    except Error as e:
                print("Error de conexión con MySQL", e)

    return geo
        


# In[230]:


def etl_product(ruta4):
    data_products = pd.read_csv(ruta4)
    data_products.drop(columns=['product_name_lenght', 'product_description_lenght'], axis=1, inplace=True)
    ## UTILIZAR FUNCION DE ETL_CATEGORYPRODUCT
    datamerge = merge1
    data_products.drop(columns=['product_photos_qty'], inplace=True)
    data_products['product_category_name'] = data_products['product_category_name'].fillna('sin_categoria')
    data_products = pd.merge(datamerge,data_products)
    data_products.drop(columns='product_category_name', inplace=True)
    data_products.drop(columns='idproduct_name', inplace= True)

    #Utilizamos la media para rellenar espacios nulos
    product_weight_median = data_products['product_weight_g'].median()
    product_length_median = data_products['product_length_cm'].median()
    product_height_median = data_products['product_height_cm'].median()
    product_width_median = data_products['product_width_cm'].median()
    data_products['product_weight_g'].fillna(product_weight_median, inplace=True)
    data_products['product_length_cm'].fillna(product_length_median, inplace=True)
    data_products['product_height_cm'].fillna(product_height_median, inplace=True)
    data_products['product_width_cm'].fillna(product_width_median, inplace=True)    



    try:
        conn = msql.connect(host= host, database= database, user= user, password= password)
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            cursor.execute('DROP TABLE IF EXISTS product;')
            cursor.execute("CREATE TABLE product (product_category_name_english varchar(255),product_id varchar(255), product_weight_g FLOAT(10), product_length_cm FLOAT(10), product_height_cm FLOAT(10), product_width_cm FLOAT(10), PRIMARY KEY (product_id))")
            for i,row in data_products.iterrows():
                sql = "INSERT INTO product VALUES (%s,%s,%s,%s,%s,%s)"
                cursor.execute(sql, tuple(row))
                conn.commit()


    except Error as e:
                print("Error while connecting to MySQL", e)


# In[227]:



# In[171]:


def etl_payment(ruta5):
    data_payment = pd.read_csv(ruta5)
    data_payment['payment_type'] = data_payment['payment_type'].replace('boleto', 'voucher')

    data_payment.reset_index(inplace=True)
    data_payment.rename(columns={'index':'payment_id'}, inplace=True)
    data_payment['payment_id'] = data_payment['payment_id'].astype(str)



    try:
        conn = msql.connect(host= host, database= database, user= user, password= password)
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            cursor.execute('DROP TABLE IF EXISTS payment;')
            cursor.execute("CREATE TABLE payment(payment_id INT(11), order_id varchar(255),payment_sequential INT(5), payment_type VARCHAR(50), payment_installments INT(5), payment_value FLOAT(12), PRIMARY KEY(payment_id))")
            for i,row in data_payment.iterrows():
                sql = "INSERT INTO payment VALUES (%s,%s,%s,%s,%s,%s)"
                cursor.execute(sql, tuple(row))
                conn.commit()
            

            

    except Error as e:
                print("Error while connecting to MySQL", e)


# In[214]:


def etl_order(ruta6):
    data_order = pd.read_csv(ruta6)
    date_cols = ['order_purchase_timestamp','order_approved_at','order_delivered_carrier_date','order_delivered_customer_date','order_estimated_delivery_date']

    for j in date_cols:
        data_order[j] = pd.to_datetime(data_order[j])
    data_order.drop([47552], inplace= True)
    diferencia_compra_aprobacion = data_order['order_approved_at'] - data_order['order_purchase_timestamp']
    diferencia_aprobacion_envio = data_order['order_delivered_carrier_date'] - data_order['order_approved_at']

    Q1 = diferencia_compra_aprobacion.dt.days.quantile(0.25) 
    Q3 = diferencia_compra_aprobacion.dt.days.quantile(0.75)
    IQR = Q3 - Q1
    BI = Q1 - 1.5*IQR
    BS = Q3 + 1.5*IQR
    out = (diferencia_compra_aprobacion.dt.days<BI) | (diferencia_compra_aprobacion.dt.days>BS)

    promedio_demora_aprobacion = diferencia_compra_aprobacion[~out].mean()

    data_order.loc[diferencia_aprobacion_envio.dt.days < 0, 'order_approved_at'] = data_order.order_purchase_timestamp + pd.Timedelta(promedio_demora_aprobacion) 

    diferencia_aprobacion_envio_2 = data_order['order_delivered_carrier_date'] - data_order['order_approved_at']

    Q1 = diferencia_aprobacion_envio_2.dt.days.quantile(0.25) 
    Q3 = diferencia_aprobacion_envio_2.dt.days.quantile(0.75)
    IQR = Q3 - Q1
    BI = Q1 - 1.5*IQR
    BS = Q3 + 1.5*IQR
    out = (diferencia_aprobacion_envio_2.dt.days<BI) | (diferencia_aprobacion_envio_2.dt.days>BS)

    promedio_demora_salida = diferencia_aprobacion_envio_2[~out].mean()

    data_order.loc[diferencia_aprobacion_envio_2.dt.days < 0, 'order_delivered_carrier_date'] = data_order.order_approved_at + pd.Timedelta(promedio_demora_salida) 

    diferencia_salida_llegada = data_order['order_delivered_customer_date'] - data_order['order_delivered_carrier_date']

    Q1 = diferencia_salida_llegada.dt.days.quantile(0.25) 
    Q3 = diferencia_salida_llegada.dt.days.quantile(0.75)
    IQR = Q3 - Q1
    BI = Q1 - 1.5*IQR
    BS = Q3 + 1.5*IQR
    out = (diferencia_salida_llegada.dt.days<BI) | (diferencia_salida_llegada.dt.days>BS)

    promedio_demora_llegada = diferencia_salida_llegada[~out].mean()

    data_order.loc[diferencia_salida_llegada.dt.days < 0, 'order_delivered_customer_date'] = data_order.order_delivered_carrier_date + pd.Timedelta(promedio_demora_llegada) 
    
    data_order.loc[(data_order.order_status == 'delivered') & (data_order.order_approved_at.isnull()), 'order_approved_at'] = data_order.order_purchase_timestamp + pd.Timedelta(promedio_demora_aprobacion) 

    data_order.loc[(data_order.order_status == 'delivered') & (data_order.order_delivered_carrier_date.isnull()), 'order_delivered_carrier_date'] = data_order.order_approved_at + pd.Timedelta(promedio_demora_salida) 

    data_order.loc[(data_order.order_status == 'delivered') & (data_order.order_delivered_customer_date.isnull()), 'order_delivered_customer_date'] = data_order.order_delivered_carrier_date + pd.Timedelta(promedio_demora_llegada) 

    lista = ['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date']

    for i in lista:
        data_order[i] = pd.to_datetime(data_order[i]).round('s')

    data_order.replace({np.nan: None}, inplace= True)

    try:
        conn = msql.connect(host= host, database= database, user= user, password= password)
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            cursor.execute("DROP TABLE IF EXISTS table_order;")
            cursor.execute("CREATE TABLE table_order (order_id VARCHAR (255), customer_id VARCHAR (255), order_status VARCHAR (255), order_purchase_timestamp DATETIME(0),\
                order_approved_at DATETIME(0), order_delivered_carrier_date DATETIME(0), order_delivered_customer_date DATETIME(0),\
                    order_estimated_delivery_date DATETIME(0), PRIMARY KEY (order_id))")
            
            for i,row in data_order.iterrows():
                #here %S means string values
                # Habria que chequear aca si funciona con los Ints o los Floats o Fechas,inclusive. Siempre creo que se usa el %s.
                sql = "INSERT INTO table_order VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
                cursor.execute(sql, tuple(row))
                # the connection is not auto committed by default, so we must commit to save our changes
                conn.commit()



    except Error as e:
                print("Error while connecting to MySQL", e)    


# In[173]:


def etl_review(ruta7):
    data_order_review = pd.read_csv(ruta7)
    data_order_review['review_creation_date'] = pd.to_datetime(data_order_review['review_creation_date'])
    data_order_review['review_id'] = data_order_review.index
    data_order_review['review_id'] = data_order_review['review_id'].astype(str)
    data_order_review.drop(columns=['review_comment_title', 'review_comment_message', 'review_answer_timestamp'], inplace=True)


    try:
        conn = msql.connect(host= host, database= database, user= user, password= password)
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            cursor.execute("DROP TABLE IF EXISTS review;")
            cursor.execute("CREATE TABLE review (review_id INT(10), order_id VARCHAR (255), review_score INT(10), review_creation_date DATE, PRIMARY KEY (review_id))")
                

            for i,row in data_order_review.iterrows():
                #here %S means string values
                # Habria que chequear aca si funciona con los Ints o los Floats o Fechas,inclusive. Siempre creo que se usa el %s.
                sql = "INSERT INTO review VALUES (%s,%s,%s, %s)"
                cursor.execute(sql, tuple(row))
                # the connection is not auto committed by default, so we must commit to save our changes
                conn.commit()


    except Error as e:
                print("Error while connecting to MySQL", e)


# In[174]:


def etl_items(ruta8):
    data_order= pd.read_csv(ruta8)
    data_order['shipping_limit_date'] = pd.to_datetime(data_order['shipping_limit_date'])
    data_order['percentagePF'] = ( data_order['freight_value'] * 100 ) / data_order['price']
    data_order.rename(columns= {'order_item_id' : 'quantity'}, inplace= True)
    data_order = data_order.reindex(columns = ['order_id', 'product_id', 'seller_id', 'shipping_limit_date', 'quantity', 'price', 'freight_value', 'percentagePF'])
    data_order.reset_index(inplace=True)
    data_order.rename(columns={'index':'id_orderitem'}, inplace=True)



    try:
        conn = msql.connect(host= host, database= database, user= user, password= password)
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            cursor.execute('DROP TABLE IF EXISTS order_items;')
            cursor.execute("CREATE TABLE order_items(id_orderitem INT(11), order_id varchar(255),product_id varchar(255), seller_id varchar(255), shipping_limit_date DATETIME(0), quantity INT(5), price FLOAT(10), freight_value FLOAT(10), percentagePF FLOAT(10), PRIMARY KEY (id_orderitem))")
            for i,row in data_order.iterrows():
                #here %S means string values
                # Habria que chequear aca si funciona con los Ints o los Floats o Fechas,inclusive. Siempre creo que se usa el %s.
                sql = "INSERT INTO order_items VALUES (%s,%s,%s,%s,%s,%s,%s,%s, %s)"
                cursor.execute(sql, tuple(row))
                # the connection is not auto committed by default, so we must commit to save our changes
                conn.commit()

    except Error as e:
                print("Error while connecting to MySQL", e)    


# In[182]:


def etl_seller(ruta9):
    
    df = pd.read_csv(ruta9)
    df.rename(columns = {'seller_zip_code_prefix': 'zip_code', 'seller_city': 'city', 'seller_state': 'state'}, inplace= True)
    df['city'] = df['city'].str.title()
    df['zip_code'] = df['zip_code'].astype(str)
    ## AGREGAR RUTA
    #dfgeo = pd.read_parquet(r'C:\Users\leand\Desktop\Data Science (Henry)\Proyectos\Proyecto_grupal\DS-Proyecto_Grupal_Olist\data_parquet\geolocation_cleanP')
    dfgeo = merge_geo
    df = df[['seller_id', 'zip_code']].merge(dfgeo[['zip_code', 'city', 'state_name']], on='zip_code')



    try:
        conn = msql.connect(host= host, database= database, user= user, password= password)
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            cursor.execute('DROP TABLE IF EXISTS seller;')
            cursor.execute("CREATE TABLE seller(seller_id VARCHAR(255), zip_code INT(10), city VARCHAR(255), state_name VARCHAR(255), PRIMARY KEY(seller_id))")
            for i,row in df.iterrows():
                #here %S means string values
                # Habria que chequear aca si funciona con los Ints o los Floats o Fechas,inclusive. Siempre creo que se usa el %s.
                sql = "INSERT INTO seller VALUES (%s,%s,%s,%s)"
                cursor.execute(sql, tuple(row))
                # the connection is not auto committed by default, so we must commit to save our changes
                conn.commit()


    except Error as e:
                print("Error while connecting to MySQL", e)


# In[183]:


def etl_customer(ruta10):
    ##UTILIZA GEO
    df = pd.read_csv(ruta10)
    dfgeo = merge_geo
    df.rename(columns = {'customer_zip_code_prefix': 'zip_code', 'customer_city': 'city', 'customer_state': 'state'}, inplace= True)
    df['city'] = df['city'].str.title()
    df.rename(columns={'customer_zip_code_prefix': 'zip_code'}, inplace=True)
    df['zip_code'] = df['zip_code'].astype(str)
    df = df[['customer_id', 'customer_unique_id', 'zip_code']].merge(dfgeo[['zip_code', 'city', 'state_name']], on='zip_code')



    try:
        conn = msql.connect(host= host, database= database, user= user, password= password)
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            cursor.execute("DROP TABLE IF EXISTS customer;")
            cursor.execute("CREATE TABLE customer (customer_id VARCHAR (255), customer_unique_id VARCHAR (255), zip_code INT(10), city VARCHAR (255), state_name VARCHAR(255), PRIMARY KEY (customer_id))")
            
            for i,row in df.iterrows():
                #here %S means string values
                # Habria que chequear aca si funciona con los Ints o los Floats o Fechas,inclusive. Siempre creo que se usa el %s.
                sql = "INSERT INTO customer VALUES (%s,%s,%s,%s,%s)"
                cursor.execute(sql, tuple(row))
                # the connection is not auto committed by default, so we must commit to save our changes
                conn.commit()


    except Error as e:
                print("Error while connecting to MySQL", e)


# In[ ]:


funciones = [etl_categoryproduct, etl_closed, etl_geolocalizacion, etl_product, etl_payment, etl_order, etl_review, etl_items, etl_seller, etl_customer]
rutas = [ruta1, ruta2, ruta3, ruta4, ruta5, ruta6, ruta7, ruta8, ruta9, ruta10]
j = 0
p = 0

for i in funciones:
   print('Normalizando el archivo', rutas[j].split(carpeta_datos)[1], 'y cargando en base de datos...')
   
   if i == etl_geolocalizacion:
      merge_geo = i(rutas[j], rutaestados)
   elif i == etl_categoryproduct:
      merge1 = i(rutas[j])
   else:
      i(rutas[j])

   j = j + 1
   p = p + 1
   
print('El proceso ETL ha finalizado exitosamente')


# In[200]:


try:
    conn = msql.connect(host= host, database= database, user= user, password= password)
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        cursor.execute("ALTER TABLE category_product ADD INDEX(product_category_name_english);")
        cursor.execute("ALTER TABLE closed_deals ADD INDEX(seller_id);")
        cursor.execute("ALTER TABLE customer ADD INDEX(zip_code);")
        cursor.execute("ALTER TABLE order_items ADD INDEX(order_id);")
        cursor.execute("ALTER TABLE order_items ADD INDEX(product_id);")
        cursor.execute("ALTER TABLE order_items ADD INDEX(seller_id);")
        cursor.execute("ALTER TABLE payment ADD INDEX(order_id);")
        cursor.execute("ALTER TABLE product ADD INDEX(product_category_name_english);")
        cursor.execute("ALTER TABLE review ADD INDEX(order_id);")
        cursor.execute("ALTER TABLE seller ADD INDEX(zip_code);")
        cursor.execute("ALTER TABLE seller ADD INDEX(seller_id);")
        cursor.execute("ALTER TABLE table_order ADD INDEX(customer_id);")
        cursor.execute("ALTER TABLE table_order ADD INDEX(order_id);")


except Error as e:
            print("Error while connecting to MySQL", e)


# In[201]:


try:
    conn = msql.connect(host= host, database= database, user= user, password= password)
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        cursor.execute("ALTER TABLE category_product ADD CONSTRAINT category_product_fk_product FOREIGN KEY (product_category_name_english) REFERENCES product (product_category_name_english) ON DELETE RESTRICT ON UPDATE RESTRICT;")
        cursor.execute("ALTER TABLE closed_deals ADD CONSTRAINT closed_deals_fk_seller FOREIGN KEY (seller_id) REFERENCES seller (seller_id) ON DELETE RESTRICT ON UPDATE RESTRICT;")
        cursor.execute("ALTER TABLE customer ADD CONSTRAINT customer_fk_geolocation FOREIGN KEY (zip_code) REFERENCES geolocation (zip_code) ON DELETE RESTRICT ON UPDATE RESTRICT;")
        cursor.execute("ALTER TABLE order_items ADD CONSTRAINT order_items_fk_table_order FOREIGN KEY (order_id) REFERENCES table_order (order_id) ON DELETE RESTRICT ON UPDATE RESTRICT;")
        cursor.execute("ALTER TABLE order_items ADD CONSTRAINT order_items_fk_product FOREIGN KEY (product_id) REFERENCES product (product_id) ON DELETE RESTRICT ON UPDATE RESTRICT;")
        cursor.execute("ALTER TABLE order_items ADD CONSTRAINT order_items_fk_seller FOREIGN KEY (seller_id) REFERENCES seller (seller_id) ON DELETE RESTRICT ON UPDATE RESTRICT;")
        cursor.execute("ALTER TABLE payment ADD CONSTRAINT payment_fk_table_order FOREIGN KEY (order_id) REFERENCES table_order (order_id) ON DELETE RESTRICT ON UPDATE RESTRICT;")
        cursor.execute("ALTER TABLE product ADD CONSTRAINT product_fk_category_product FOREIGN KEY (product_category_name_english) REFERENCES category_product (product_category_name_english) ON DELETE RESTRICT ON UPDATE RESTRICT;")
        cursor.execute("ALTER TABLE review ADD CONSTRAINT review_fk_table_order FOREIGN KEY (order_id) REFERENCES table_order (order_id) ON DELETE RESTRICT ON UPDATE RESTRICT;")
        cursor.execute("ALTER TABLE seller ADD CONSTRAINT seller_fk_geolocation FOREIGN KEY (zip_code) REFERENCES geolocation (zip_code) ON DELETE RESTRICT ON UPDATE RESTRICT;")
        cursor.execute("ALTER TABLE table_order ADD CONSTRAINT table_order_fk_customer FOREIGN KEY (customer_id) REFERENCES customer (customer_id) ON DELETE RESTRICT ON UPDATE RESTRICT;")


except Error as e:
            print("Error while connecting to MySQL", e)

