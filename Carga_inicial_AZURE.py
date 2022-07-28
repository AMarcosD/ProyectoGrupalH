
import pandas as pd
import numpy as np
from datetime import datetime
import os
from geopy.geocoders import Nominatim 
geolocator = Nominatim(user_agent="MakroAnalyse")
import psycopg2
from psycopg2 import Error
import psycopg2.extras  


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


carpeta_normalizados = r'C:\Users\PC\Desktop\ProyectoGrupalH\data_normalizada'

database = 'dbt'
user = 'Grupo5@olist-db-pg5'
password = 'Datascience22'
host = 'olist-db-pg5.postgres.database.azure.com'
port = '5432'


def etl_seller(ruta9):
    
    df = pd.read_csv(ruta9)
    df.rename(columns = {'seller_zip_code_prefix': 'zip_code', 'seller_city': 'city', 'seller_state': 'state'}, inplace= True)
    df['city'] = df['city'].str.title()
    df['zip_code'] = df['zip_code'].astype(str)
    dfgeo = merge_geo
    df = df[['seller_id', 'zip_code']].merge(dfgeo[['zip_code', 'city', 'state_name']], on='zip_code')

      
    archivos = os.listdir(carpeta_normalizados)

    if 'seller.csv' in archivos:
        pass
    else:
        ruta = r'C:\Users\PC\Desktop\ProyectoGrupalH\data_normalizada\seller.csv'
        df.to_csv(ruta, index=False)
        try:
            conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()
            my_file = open(ruta)
            SQL_STATEMENT = "COPY seller FROM stdin DELIMITER ',' CSV header;"
            cursor.copy_expert(sql=SQL_STATEMENT, file= my_file)
            conn.commit()
        except Exception as error:
            print(error)


def etl_customer(ruta10):
    
    df = pd.read_csv(ruta10)
    dfgeo = merge_geo
    df.rename(columns = {'customer_zip_code_prefix': 'zip_code', 'customer_city': 'city', 'customer_state': 'state'}, inplace= True)
    df['city'] = df['city'].str.title()
    df.rename(columns={'customer_zip_code_prefix': 'zip_code'}, inplace=True)
    df['zip_code'] = df['zip_code'].astype(str)
    df = df[['customer_id', 'customer_unique_id', 'zip_code']].merge(dfgeo[['zip_code', 'city', 'state_name']], on='zip_code')

    archivos = os.listdir(carpeta_normalizados)
    
    if 'customer.csv' in archivos:
        now = datetime.now()
        x = str(now.day) + '_' + str(now.month) + '_' + str(now.year)
        df.to_csv(r"C:\Users\PC\Desktop\ProyectoGrupalH\data_normalizada\customer{}.csv".format(x),index=False)
        try: 
            conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE table_aux(customer_id varchar, customer_unique_id varchar, zip_code integer, city varchar, state_name varchar, PRIMARY KEY (customer_id));")

            conn.commit()

        except (Exception, psycopg2.Error) as error:
            print(error)

        try:
            #TRAIGO EL ARCHIVO DELTA NORMALIZADO
            now = datetime.now()
            x = str(now.day) + '_' + str(now.month) + '_' + str(now.year)
            my_file = open(r"C:\Users\PC\Desktop\ProyectoGrupalH\data_normalizada\customer{}.csv".format(x))

            conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()

            SQL_STATEMENT = "COPY table_aux FROM stdin DELIMITER ',' CSV header;"
            cursor.copy_expert(sql=SQL_STATEMENT, file= my_file)
            conn.commit()

        except (Exception, psycopg2.Error) as error:
            print(error)

        try:
            
            conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()
            cursor.execute("INSERT INTO customer(customer_id , customer_unique_id , zip_code , city , state_name)\
            SELECT customer_id , customer_unique_id , zip_code , city , state_name \
               FROM (SELECT * FROM table_aux\
            WHERE table_aux.customer_id NOT IN (SELECT customer_id FROM customer)) as delta;;")

            cursor.execute("DROP TABLE IF EXISTS table_aux;")

            conn.commit()
            os.remove(r"C:\Users\PC\Desktop\ProyectoGrupalH\Delta\customer.csv")

        except (Exception, psycopg2.Error) as error:
            print(error)
    else:
        ruta = r'C:\Users\PC\Desktop\ProyectoGrupalH\data_normalizada\customer.csv'
        df.to_csv(ruta, index=False)
        try:
            conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()
            my_file = open(ruta)
            SQL_STATEMENT = "COPY customer FROM stdin DELIMITER ',' CSV header;"
            cursor.copy_expert(sql=SQL_STATEMENT, file= my_file)
            conn.commit()
        except Exception as error:
            print(error)


def etl_product(ruta4):
    data_products = pd.read_csv(ruta4)
    data_products.drop(columns=['product_name_lenght', 'product_description_lenght'], axis=1, inplace=True)
    ## UTILIZAR FUNCION DE ETL_CATEGORYPRODUCT
    datamerge = merge1
    data_products.drop(columns=['product_photos_qty'], inplace=True)
    data_products['product_category_name'] = data_products['product_category_name'].fillna('sin_categoria')
    data_products = pd.merge(datamerge, data_products)
    data_products.drop(columns='product_category_name', inplace=True)
    data_products.drop(columns='product_name_id', inplace= True)

    #Utilizamos la media para rellenar espacios nulos
    product_weight_median = data_products['product_weight_g'].median()
    product_length_median = data_products['product_length_cm'].median()
    product_height_median = data_products['product_height_cm'].median()
    product_width_median = data_products['product_width_cm'].median()
    data_products['product_weight_g'].fillna(product_weight_median, inplace=True)
    data_products['product_length_cm'].fillna(product_length_median, inplace=True)
    data_products['product_height_cm'].fillna(product_height_median, inplace=True)
    data_products['product_width_cm'].fillna(product_width_median, inplace=True)   
    data_products = data_products.reindex(columns = ['product_id', 'product_category_name_english', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm'])

    
   
    archivos = os.listdir(carpeta_normalizados)
    if 'product.csv' in archivos:
        pass
    else:
        ruta = r'C:\Users\PC\Desktop\ProyectoGrupalH\data_normalizada\product.csv'
        data_products.to_csv(ruta, index=False)
        try:
            conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()
            my_file = open(ruta)
            SQL_STATEMENT = "COPY product FROM stdin DELIMITER ',' CSV header;"
            cursor.copy_expert(sql=SQL_STATEMENT, file= my_file)
            conn.commit()
        except Exception as error:
            print(error)



def etl_order(ruta6):
    data_order = pd.read_csv(ruta6)
    date_cols = ['order_purchase_timestamp','order_approved_at','order_delivered_carrier_date','order_delivered_customer_date','order_estimated_delivery_date']

    for j in date_cols:
        data_order[j] = pd.to_datetime(data_order[j])
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

    
    
    archivos = os.listdir(carpeta_normalizados)
        
    if 'data_order.csv' in archivos:
        now = datetime.now()
        x = str(now.day) + '_' + str(now.month) + '_' + str(now.year)
        data_order.to_csv(r"C:\Users\PC\Desktop\ProyectoGrupalH\data_normalizada\data_order{}.csv".format(x),index=False)
        try: 
            conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE table_aux (order_id varchar, customer_id varchar, order_status varchar, order_purchase_timestamp timestamp,\
            order_approved_at timestamp, order_delivered_carrier_date timestamp, order_delivered_customer_date timestamp,\
            order_estimated_delivery_date timestamp, PRIMARY KEY (order_id));")

            conn.commit()

        except (Exception, psycopg2.Error) as error:
            print(error)
            
        try:
            #TRAIGO EL ARCHIVO DELTA NORMALIZADO
            now = datetime.now()
            x = str(now.day) + '_' + str(now.month) + '_' + str(now.year)
            my_file = open(r"C:\Users\PC\Desktop\ProyectoGrupalH\data_normalizada\data_order{}.csv".format(x))

            conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()

            SQL_STATEMENT = "COPY table_aux FROM stdin DELIMITER ',' CSV header;"
            cursor.copy_expert(sql=SQL_STATEMENT, file= my_file)
            
            conn.commit()

        except (Exception, psycopg2.Error) as error:
            print(error)

        try:
            # INSERTAMOS LOS AUXILIARES EN ORDENES DONDE NO SE REPITA ORDER_ID
            
            conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()
            cursor.execute("INSERT INTO customer (customer_id) SELECT customer_id\
                 FROM (SELECT DISTINCT customer_id FROM table_aux\
            WHERE table_aux.customer_id NOT IN (SELECT customer_id FROM customer)) as c_ini;")

            cursor.execute("INSERT INTO data_order (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at,\
                order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date)\
            SELECT order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, \
                order_delivered_customer_date, order_estimated_delivery_date FROM (SELECT * FROM table_aux\
            WHERE table_aux.order_id NOT IN (SELECT order_id FROM data_order)) as delta;")

            cursor.execute("DROP TABLE IF EXISTS table_aux;")

            conn.commit()
            os.remove(r"C:\Users\PC\Desktop\ProyectoGrupalH\Delta\data_order.csv")

        except (Exception, psycopg2.Error) as error:
            print(error)

    else:
        data_order.to_csv(r'C:\Users\PC\Desktop\ProyectoGrupalH\data_normalizada\data_order.csv',index=False)
        try: 
            conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE table_aux (order_id varchar, customer_id varchar, order_status varchar, order_purchase_timestamp timestamp,\
                order_approved_at timestamp, order_delivered_carrier_date timestamp, order_delivered_customer_date timestamp,\
                    order_estimated_delivery_date timestamp, PRIMARY KEY (order_id));")

            conn.commit()

        except (Exception, psycopg2.Error) as error:
                print(error)
        try:
            
            my_file = open(r'C:\Users\PC\Desktop\ProyectoGrupalH\data_normalizada\data_order.csv')
            conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()

            SQL_STATEMENT = "COPY table_aux FROM stdin DELIMITER ',' CSV header;"
            cursor.copy_expert(sql=SQL_STATEMENT, file= my_file)
            conn.commit()
            conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()
            cursor.execute("insert into customer SELECT customer_id FROM (select d.customer_id\
            from table_aux as d\
            LEFT JOIN customer as c\
            on d.customer_id = c.customer_id\
            where c.customer_id is null) as carga;")
            cursor.execute("DROP TABLE IF EXISTS table_aux;")
            my_file = open(r'C:\Users\PC\Desktop\ProyectoGrupalH\data_normalizada\data_order.csv')
            SQL_STATEMENT = "COPY data_order FROM stdin DELIMITER ',' CSV header;"
            cursor.copy_expert(sql=SQL_STATEMENT, file= my_file)

            
            
            conn.commit()

        except Exception as error:
                print(error)


def etl_payment(ruta5):
    data_payment = pd.read_csv(ruta5)
    data_payment['payment_type'] = data_payment['payment_type'].replace('boleto', 'voucher')

    data_payment.reset_index(inplace=True)
    data_payment.rename(columns={'index':'payment_id'}, inplace=True)
    data_payment['payment_id'] = data_payment['payment_id'].astype(str)

    
    archivos = os.listdir(carpeta_normalizados)

    if 'payment.csv' in archivos:
        pass
    else:
        ruta = r'C:\Users\PC\Desktop\ProyectoGrupalH\data_normalizada\payment.csv'
        data_payment.to_csv(ruta, index=False)
        try:
            conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()
            my_file = open(ruta)
            SQL_STATEMENT = "COPY payment FROM stdin DELIMITER ',' CSV header;"
            cursor.copy_expert(sql=SQL_STATEMENT, file= my_file)
            conn.commit()
            
        except (Exception, psycopg2.Error) as error:
            print(error)


def etl_categoryproduct(ruta1):

    data_category_product = pd.read_csv(ruta1)
    data_category_product.loc[71] = ['sin_categoria','NoCategory']
    data_category_product.reset_index(inplace=True)
    data_category_product.rename(columns={'index':'product_name_id'}, inplace=True)

    
    archivos = os.listdir(carpeta_normalizados)
 
    if 'category_product.csv' in archivos:
        pass
    else:
        ruta = r'C:\Users\PC\Desktop\ProyectoGrupalH\data_normalizada\category_product.csv'
        data_category_product.to_csv(ruta, index=False)
        try:
            conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()
            my_file = open(ruta)
            SQL_STATEMENT = "COPY category_product FROM stdin DELIMITER ',' CSV header;"
            cursor.copy_expert(sql=SQL_STATEMENT, file= my_file)
            conn.commit()
        except Exception as error:
                print(error)
    return data_category_product


def etl_geolocalizacion(ruta3, rutaestados):
    geo = pd.read_csv(ruta3)
    geo.drop_duplicates(inplace=True)
    geo = geo.rename(columns={'geolocation_zip_code_prefix': 'zip_code', 'geolocation_lat':'geolatitud', \
        'geolocation_lng':'geolongitud', 'geolocation_city':'city', 'geolocation_state' : 'state'})
    dfj = pd.read_json(rutaestados)
    
    geo = geo.merge(dfj[['nome', 'sigla']], right_on='sigla', left_on='state')

    geo.rename(columns={'nome':'state_name'}, inplace=True)

    geo['city'] = geo['city'].str.title()

    geo = geo.drop_duplicates('zip_code')

    geo = geo.reset_index(drop=True)

    geo = geo[['zip_code', 'geolatitud', 'geolongitud', 'city', 'state_name']]


    geo.loc[geo.geolongitud > 0, 'geolongitud'] = geo.geolongitud * (-1)
    geo.loc[geo.geolatitud > 5, 'geolatitud'] = geo.geolatitud * (-1)


    a = (geo.geolongitud < -73) | (geo.geolongitud > -34) | (geo.geolatitud < -33)

    geo['full_data'] = geo.state_name + ',' + geo.city 

    geo.loc[a, 'gcode'] = geo[a]['full_data'].apply(geolocator.geocode)
    geo.loc[a,'lat'] = [g.latitude for g in geo[a]['gcode']]
    geo.loc[a,'long'] = [g.longitude for g in geo[a]['gcode']]
    
    geo.loc[a, 'geolatitud'] = geo[a]['lat']
    geo.loc[a, 'geolongitud'] = geo[a]['long']
    
    geo.drop(columns= ['gcode', 'lat', 'long', 'full_data'], inplace=True)



    geo['zip_code'] = geo['zip_code'].astype(str)

    
    archivos = os.listdir(carpeta_normalizados)

    if 'geolocation.csv' in archivos:
        pass
    else:
        ruta = r'C:\Users\PC\Desktop\ProyectoGrupalH\data_normalizada\geolocation.csv'
        geo.to_csv(ruta, index=False)
        try:
            conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()
            my_file = open(ruta)
            SQL_STATEMENT = "COPY geolocation FROM stdin DELIMITER ',' CSV header;"
            cursor.copy_expert(sql=SQL_STATEMENT, file= my_file)
            conn.commit()
        except Exception as error:
            print(error)
    return geo


def etl_review(ruta7):
    data_order_review = pd.read_csv(ruta7)
    data_order_review['review_creation_date'] = pd.to_datetime(data_order_review['review_creation_date'])
    data_order_review['review_id'] = data_order_review.index
    data_order_review['review_id'] = data_order_review['review_id'].astype(str)
    data_order_review.drop(columns=['review_comment_title', 'review_comment_message', 'review_answer_timestamp'], inplace=True)

    
    archivos = os.listdir(carpeta_normalizados)

    if 'data_review.csv' in archivos:
        now = datetime.now()
        x = str(now.day) + '_' + str(now.month) + '_' + str(now.year)
        data_order_review.to_csv(r"C:\Users\PC\Desktop\ProyectoGrupalH\data_normalizada\data_review{}.csv".format(x),index=False)
        try: 
            conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE table_aux(review_id integer, order_id varchar, review_score integer, review_creation_date date, PRIMARY KEY (review_id));")

            conn.commit()

        except (Exception, psycopg2.Error) as error:
            print(error)

        try:
            #TRAIGO EL ARCHIVO DELTA NORMALIZADO
                now = datetime.now()
                x = str(now.day) + '_' + str(now.month) + '_' + str(now.year)
                my_file = open(r"C:\Users\PC\Desktop\ProyectoGrupalH\data_normalizada\data_review{}.csv".format(x))

                conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
                onn = psycopg2.connect(conn_string)
                cursor = conn.cursor()

                SQL_STATEMENT = "COPY table_aux FROM stdin DELIMITER ',' CSV header;"
                cursor.copy_expert(sql=SQL_STATEMENT, file= my_file)
                conn.commit()

        except (Exception, psycopg2.Error) as error:
            print(error)
        try:
            
            conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM table_aux\
            WHERE order_id=ANY(SELECT order_id\
            FROM (SELECT DISTINCT order_id FROM table_aux\
            WHERE table_aux.order_id NOT IN (SELECT order_id FROM data_order)) as query);")

            cursor.execute("INSERT INTO data_review (review_id, order_id, review_score, review_creation_date)\
            SELECT review_id, order_id, review_score, review_creation_date \
               FROM (SELECT * FROM table_aux\
            WHERE table_aux.review_id NOT IN (SELECT review_id FROM data_review)) as delta;;")

            cursor.execute("DROP TABLE IF EXISTS table_aux;")

            conn.commit()
            os.remove(r"C:\Users\PC\Desktop\ProyectoGrupalH\Delta\data_review.csv")

        except (Exception, psycopg2.Error) as error:
            print(error)
        
    else:
        ruta = r'C:\Users\PC\Desktop\ProyectoGrupalH\data_normalizada\data_review.csv'
        data_order_review.to_csv(ruta, index=False)
        try:
            conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()
            my_file = open(ruta)
            SQL_STATEMENT = "COPY data_review FROM stdin DELIMITER ',' CSV header;"
            cursor.copy_expert(sql=SQL_STATEMENT, file= my_file)
            conn.commit()
        except Exception as error:
            print(error)


def etl_closed(ruta2):
    
    data_closed = pd.read_csv(ruta2)
    data_closed['won_date'] = pd.to_datetime(data_closed['won_date'])
    data_closed.drop(columns=['declared_monthly_revenue', 'sr_id', 'sdr_id', 'lead_behaviour_profile', \
        'has_company','lead_type','has_gtin','business_type' ,'average_stock', 'declared_product_catalog_size'], inplace=True)
    data_closed.reset_index(inplace=True)
    data_closed.rename(columns={'index':'closed_id'}, inplace=True)
    data_closed.dropna(inplace=True)

       
   
    archivos = os.listdir(carpeta_normalizados)

     
    if 'closed_deal.csv' in archivos:
            pass
    else:
        data_closed.to_csv(r'C:\Users\PC\Desktop\ProyectoGrupalH\data_normalizada\closed_deal.csv',index=False)
        try: 
            conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE table_aux (closed_id integer, mql_id varchar, seller_id varchar, won_date timestamp, business_segment varchar);")

            conn.commit()

        except (Exception, psycopg2.Error) as error:
                print(error)
            
        try:
            
            my_file = open(r'C:\Users\PC\Desktop\ProyectoGrupalH\data_normalizada\closed_deal.csv')
            conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()

            SQL_STATEMENT = "COPY table_aux FROM stdin DELIMITER ',' CSV header;"
            cursor.copy_expert(sql=SQL_STATEMENT, file= my_file)
            
            conn.commit()

        except Exception as error:
                print(error)
        
        try:
           
            
            conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()


            cursor.execute("INSERT INTO seller (seller_id) SELECT seller_id\
                 FROM (SELECT * FROM table_aux\
            WHERE table_aux.seller_id NOT IN (SELECT seller_id FROM seller)) as c_ini;")

            cursor.execute("DROP TABLE IF EXISTS table_aux;")
            my_file = open(r'C:\Users\PC\Desktop\ProyectoGrupalH\data_normalizada\closed_deal.csv')
            SQL_STATEMENT = "COPY closed_deal FROM stdin DELIMITER ',' CSV header;"
            cursor.copy_expert(sql=SQL_STATEMENT, file= my_file)

            conn.commit()
            

        except Exception as error:
                print(error)
        


def etl_items(ruta8):
    data_order= pd.read_csv(ruta8)
    data_order['shipping_limit_date'] = pd.to_datetime(data_order['shipping_limit_date'])
    data_order['percentagePF'] = ( data_order['freight_value'] * 100 ) / data_order['price']
    data_order.rename(columns= {'order_item_id' : 'quantity'}, inplace= True)
    data_order = data_order.reindex(columns = ['order_id', 'product_id', 'seller_id', 'shipping_limit_date', 'quantity', 'price', 'freight_value', 'percentagePF'])
    data_order.reset_index(inplace=True)
    data_order.rename(columns={'index':'orderitem_id'}, inplace=True)
    
    
    archivos = os.listdir(carpeta_normalizados)
    
    if 'item.csv' in archivos:
        pass
    else:
        data_order.to_csv(r'C:\Users\PC\Desktop\ProyectoGrupalH\data_normalizada\item.csv',index=False)
        try: 
            conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE table_aux (orderitem_id integer, order_id varchar, product_id varchar, seller_id varchar, shipping_limit_date timestamp, quantity integer, price float, freight_value float, percentagePF float);")
            my_file = open(r'C:\Users\PC\Desktop\ProyectoGrupalH\data_normalizada\item.csv')
            cursor = conn.cursor()
            SQL_STATEMENT = "COPY table_aux FROM stdin DELIMITER ',' CSV header;"
            cursor.copy_expert(sql=SQL_STATEMENT, file= my_file)
            conn.commit()

        except (Exception, psycopg2.Error) as error:
                print(error)
        try:
            conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()

            cursor.execute("INSERT INTO product (product_id) SELECT product_id\
                 FROM (SELECT DISTINCT product_id FROM table_aux\
            WHERE table_aux.product_id NOT IN (SELECT product_id FROM product)) as c_ini;")
            
            cursor.execute("INSERT INTO seller (seller_id) SELECT seller_id\
                 FROM (SELECT DISTINCT seller_id FROM table_aux\
               WHERE table_aux.seller_id NOT IN (SELECT seller_id FROM seller)) as c_ini;")
            

            cursor.execute("DROP TABLE IF EXISTS table_aux;")
            conn.commit()
            my_file = open(r'C:\Users\PC\Desktop\ProyectoGrupalH\data_normalizada\item.csv')
            SQL_STATEMENT = "COPY item FROM stdin DELIMITER ',' CSV header;"
            cursor.copy_expert(sql=SQL_STATEMENT, file= my_file)
            conn.commit()

            
            

        except Exception as error:
                print(error)


dict_fun_ruta ={}
lista_funciones = [etl_categoryproduct,etl_geolocalizacion, etl_seller, etl_closed, etl_customer, etl_order, etl_review, etl_product, etl_payment,etl_items]
lista_rutas = [ruta1,ruta3,ruta9,ruta2,ruta10,ruta6,ruta7,ruta4,ruta5,ruta8]
for funcion, ruta in zip(lista_funciones, lista_rutas):
    dict_fun_ruta[funcion] = ruta


# Puesta en acción del ETL para carga Inicial
# ** No es necesario repetir ejecución una vez creado. Por ese motivo, esta creado el 'if'. 


dir = os.listdir(r"C:\Users\PC\Desktop\ProyectoGrupalH\data_normalizada")
if len(dir) == 0:
    for x in dict_fun_ruta:
        if x == etl_geolocalizacion:
            merge_geo = x(dict_fun_ruta[x],rutaestados)
            print('va ok')
        elif x == etl_categoryproduct:
            merge1 = x(dict_fun_ruta[x])
            print('va ok')
        
        else:
            x(dict_fun_ruta[x])
            print('va ok')
else:
    print('La carga inicial ya fue realizada')
        


# Identificador de Deltas


dict_fun_delta ={}
lista_funciones = [etl_categoryproduct,etl_geolocalizacion, etl_seller, etl_closed, etl_customer, etl_order, etl_review, etl_product, etl_payment,etl_items]
lista_delta = ['category_product','geolocation', 'seller','closed_deal','customer','data_order','data_review','product','payment','item']
for delta,funcion in zip(lista_delta, lista_funciones):
    dict_fun_delta[delta] = funcion


carpeta_delta = r"C:\Users\PC\Desktop\ProyectoGrupalH\Delta"
lista_deltas = []
for file in os.listdir(carpeta_delta):
    if file.endswith('.csv'):
        lista_deltas.append(file[:-4])



#merge_geo = pd.read_csv(r"C:\Users\usuario\Desktop\Proyects\Proyecto Grupal\ProyectoGrupalH\ProyectoGrupalH\data_normalizada\geolocation.csv")


for file in lista_deltas:
    for clave in dict_fun_delta:
        if file == clave:
           #dict_fun_delta[clave](carpeta_delta + '\\' + file + '.csv')
           print(carpeta_delta + '\\' + file + '.csv')
        else:
            pass
       
            


