import os
from db import engine
import requests
from datetime import datetime

import pandas as pd

import re

today_str = datetime.today().strftime('%Y-%m-%d')
today = today_str.split('-')
year = today[0]
month = today[1]
day = today[2]

number_month_dict = {'01': 'enero', '02': 'febrero', '03': 'marzo', '04': 'abril', '05': 'mayo', '06': 'junio',
                     '07': 'julio', '08': 'agosto', '09': 'septiembre', '10': 'octubre', '11': 'noviembre',
                     '12': 'diciembre'}


def request_museos():
    """
    Esta funcion descarga el archivo CSV de museos y los almancena en la direccion museos_dir,
    con el formato “categoría/año-mes/categoria-dia-mes-año.csv”
    """
    r = requests.get("https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f"
                     "/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museo.csv")

    museos_dir = f'museos/{year}-{number_month_dict[month]}'

    # Crear de forma segura donde se almacenan los archivos csv.
    if not os.path.exists(museos_dir):
        os.makedirs(museos_dir)

    open(f'{museos_dir}/museos-{day}-{month}-{year}.csv', 'wb').write(r.content)

    df_museo = pd.read_csv(f'museos/{year}-{number_month_dict[month]}/'
                           f'museos-{day}-{month}-{year}.csv')

    df_museo = reformat_columns(data_frame=df_museo)

    # Sobreescribimos los datos formateados
    df_museo.to_csv(f'museos/{year}-{number_month_dict[month]}'
                    f'/museos-{day}-{month}-{year}.csv', index=False)


def request_salas_cines():
    """
    Esta funcion descarga el archivo CSV de salas de cines y los almancena en la direccion sala_cines_dir,
    con el formato “categoría/año-mes/categoria-dia-mes-año.csv”
    """
    r = requests.get("https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f"
                     "/resource/392ce1a8-ef11-4776-b280-6f1c7fae16ae/download/cine.csv")

    salas_cines_dir = f'salas_de_cines/{year}-{number_month_dict[month]}'

    # Crear de forma segura donde se almacenan los archivos csv.
    if not os.path.exists(salas_cines_dir):
        os.makedirs(salas_cines_dir)

    open(f'{salas_cines_dir}/salas_de_cines-{day}-{month}-{year}.csv', 'wb').write(r.content)

    # Procesamiento de datos para generar las Tablas
    df_cines = pd.read_csv(f'salas_de_cines/{year}-{number_month_dict[month]}/salas_de_cines-{day}-{month}-{year}.csv')

    df_cines = reformat_columns(data_frame=df_cines)

    df_cines.espacio_incaa = df_cines.espacio_incaa.str.upper()

    df_cines.to_csv(f'salas_de_cines/{year}-{number_month_dict[month]}'
                    f'/salas_de_cines-{day}-{month}-{year}.csv', index=False)


def request_biblioteca():
    """
    Esta funcion descarga el archivo CSV de salas de cines y los almancena en la direccion sala_cines_dir,
    con el formato “categoría/año-mes/categoria-dia-mes-año.csv”
    """
    r = requests.get("https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/"
                     "resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv")

    bibliotecas_pupulares_dir = f'bibliotecas_populares/{year}-{number_month_dict[month]}'

    # Crear de forma segura donde se almacenan los archivos csv.
    if not os.path.exists(bibliotecas_pupulares_dir):
        os.makedirs(bibliotecas_pupulares_dir)

    # Guardamos los datos crudos
    open(f'{bibliotecas_pupulares_dir}/bibliotecas_populares-{day}-{month}-{year}.csv', 'wb').write(r.content)

    # Procesamiento de datos para generar las Tablas
    df_biblio = pd.read_csv(f'bibliotecas_populares/{year}-{number_month_dict[month]}/'
                            f'bibliotecas_populares-{day}-{month}-{year}.csv')

    df_biblio = reformat_columns(data_frame=df_biblio)

    # Sobreescribimos los datos formateados
    df_biblio.to_csv(f'bibliotecas_populares/{year}-{number_month_dict[month]}'
                     f'/bibliotecas_populares-{day}-{month}-{year}.csv', index=False)


def reformat_columns(data_frame):
    """
    Recibe un dataframe, renombra las columnas para que coincidan con el resto de las categorias
    Devuelve el dataframe con las columnas formateadas
    """

    # Coloca en minusuclas todos los nombres de las columnas
    data_frame.columns = map(str.lower, data_frame.columns)

    # Agrega un guion bajo luego de encontrar 'id'
    data_frame = data_frame.rename(columns=lambda x: re.sub(r'(id)', r'\1_', x))

    dictionary = {'cod_loc': 'cod_localidad', 'localid_ad': 'localidad', 'cp':  'código postal',
                  'categoria': 'categoría', 'teléfono': 'número de teléfono', 'telefono': 'número de teléfono',
                  'dirección': 'domicilio', 'direccion': 'domicilio'}

    data_frame = data_frame.rename(columns=dictionary)

    return data_frame


def cines_to_sql():
    columns = ['provincia', 'pantallas', 'butacas', 'espacio_incaa']
    cines = pd.read_csv(f'salas_de_cines/{year}-{number_month_dict[month]}/'
                        f'salas_de_cines-{day}-{month}-{year}.csv', usecols=columns)
    cines['fecha de carga'] = today_str
    cines.to_sql('cines', con=engine, if_exists='replace', index=False)


def all_info_to_sql():
    """
    Coloca los datos de todas las categorias en una sola tabla
    """

    columns = ['cod_localidad', 'id_provincia', 'id_departamento', 'categoría', 'provincia', 'localidad', 'nombre',
               'domicilio', 'código postal', 'número de teléfono', 'mail', 'web']

    cines = pd.read_csv(f'salas_de_cines/{year}-{number_month_dict[month]}/'
                        f'salas_de_cines-{day}-{month}-{year}.csv', usecols=columns)

    bibliotecas = pd.read_csv(f'bibliotecas_populares/{year}-{number_month_dict[month]}/'
                              f'bibliotecas_populares-{day}-{month}-{year}.csv', usecols=columns)

    museos = pd.read_csv(f'museos/{year}-{number_month_dict[month]}/museos-{day}-{month}-{year}.csv', usecols=columns)

    all_info = pd.concat([cines, bibliotecas, museos])
    all_info['fecha de carga'] = today_str
    all_info.to_sql('all_info', con=engine, if_exists='replace', index=False)


def registros_sql():
    """
    Crea una tabla con cantida de registros por categoria, cantidad de registros por fuente,
    y cantida de registros por categoria y provindcia
    """
    cines = pd.read_csv(f'salas_de_cines/{year}-{number_month_dict[month]}/'
                        f'salas_de_cines-{day}-{month}-{year}.csv')

    bibliotecas = pd.read_csv(f'bibliotecas_populares/{year}-{number_month_dict[month]}/'
                              f'bibliotecas_populares-{day}-{month}-{year}.csv')

    museos = pd.read_csv(f'museos/{year}-{number_month_dict[month]}/museos-{day}-{month}-{year}.csv')

    all_info = pd.concat([cines, bibliotecas, museos])

    categorias = all_info.groupby('categoría').size()

    fuentes = all_info.groupby('fuente').size()

    categoria_provincia = all_info.groupby(['categoría', 'provincia']).size()

    tabla = pd.concat([categorias, fuentes, categoria_provincia], axis=1)

    columnas = ['Registros por categoria', 'Registros por fuente', 'Registros por categoria y provincia']

    tabla.columns = columnas

    tabla.to_sql('registros', con=engine, if_exists='replace')


if __name__ == '__main__':
    request_museos()
    request_biblioteca()
    request_salas_cines()
    cines_to_sql()
    all_info_to_sql()
    registros_sql()
