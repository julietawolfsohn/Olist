import numpy as np
import pandas as pd
from math import radians, sin, cos, asin, sqrt


def transformar_columnas_datetime(orders):
    date_columns = ['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date',
                'order_delivered_customer_date', 'order_estimated_delivery_date']

    for column in date_columns:
        orders[column] = pd.to_datetime(orders[column])
    
    return orders


def puntaje_de_compra(orders):
    orders['es_cinco_estrellas'] = orders['review_score'].apply(lambda x: 1 if x == 5 else 0)
    orders['es_una_estrella'] = orders['review_score'].apply(lambda x: 1 if x == 1 else 0)
    return orders


def load_all_data(path):
    ''' read all datasets in folder and usea as name'''
    files = [f for f in os.listdir(path) if f.endswith(".csv")]
    data = {normalize_name(filename): pd.read_csv(f"{path}/{filename}") for filename in files}
    return data


def calcular_numero_productos(df):
    conteo_por_order_id_df = df.groupby('order_id')['order_item_id'].count()
    conteo_por_order_id_df = conteo_por_order_id_df.reset_index() #me lo pasa a dataframe
    conteo_por_order_id_df = conteo_por_order_id_df.rename(columns={'order_item_id': 'count_order_items'})
    return conteo_por_order_id_df


def vendedores_unicos(df):
    conteo_por_order_id_df = df.groupby('order_id')['seller_id'].nunique()
    conteo_por_order_id_df = conteo_por_order_id_df.reset_index() #me lo pasa a dataframe
    conteo_por_order_id_df = conteo_por_order_id_df.rename(columns={'order_item_id': 'seller_id'})
    return conteo_por_order_id_df
    
def calcular_precio_y_transporte(df):
    # Hacer el groupby y contar las filas para cada 'order_id'
    conteo_por_order_id_df = df.groupby('order_id').agg({'price': 'sum', 'freight_value': 'sum'}).reset_index()

    # Renombrar las columnas
    conteo_por_order_id_df = conteo_por_order_id_df.rename(columns={'price': 'sum_price', 'freight_value': 'sum_freight_value'})

    return conteo_por_order_id_df
