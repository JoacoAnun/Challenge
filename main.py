import os
from db import engine
import requests
from datetime import datetime
import pandas as pd
import re
import logging

# Configuracion para crear el archivo test.log que almacenara los logs de eventos
logging.basicConfig(level=logging.DEBUG, filename='test.log', format='%(asctime)s:%(levelname)s:%(message)s')
# test.log almacenará también los intentos de conexion a las url
logging.getLogger("urllib3").setLevel(logging.DEBUG)

today_str = datetime.today().strftime('%Y-%m-%d')
today = today_str.split('-')
year = today[0]
month = today[1]
day = today[2]

number_month_dict = {'01': 'enero', '02': 'febrero', '03': 'marzo', '04': 'abril', '05': 'mayo', '06': 'junio',
                     '07': 'julio', '08': 'agosto', '09': 'septiembre', '10': 'octubre', '11': 'noviembre',
                     '12': 'diciembre'}

list_urls = ["https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f"
             "/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museo.csv",
             "https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f"
             "/resource/392ce1a8-ef11-4776-b280-6f1c7fae16ae/download/cine.csv",
             "https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/"
             "resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv"
             ]


def request_data(url):
    """
    Extrae los datos de la url y los almacena en la carpeta de almacenamiento correspondiente
    Args url:(lista) direccion web de los datos a extraer

    Returns: (str) direccion del archivo almacenado
    """

    # Controlamos respuesta del url provisto
    try:
        data = requests.get(url)
        data.raise_for_status()
    except requests.exceptions.HTTPError as err:
        logging.critical(err)
        raise SystemExit(err)
    except requests.exceptions.ConnectionError as err:
        logging.critical(err)
        logging.debug('Reviar conexion a internet')
        raise SystemExit(err)

    # Extraemos categoria del url
    category_name = get_category(url)
    directory = create_directory(category_name)

    full_dir = f'{directory}/{category_name}-{day}-{month}-{year}.csv'
    open(f'{full_dir}', 'wb').write(data.content)

    return full_dir


def create_directory(category_name):
    """
    Crea de forma segura el directorio para almacenar la informacion
    Args: (str) category_name, nombre de la categoria

    Returns: (str) lugar de almacenamiento de los datos
    """
    category_dir = f'{category_name}/{year}-{number_month_dict[month]}'

    if not os.path.exists(category_dir):
        os.makedirs(category_dir)
    logging.info(f'Directory for .csv: {category_dir}')
    return category_dir


def get_category(url):
    """
    Esta funcion extrae el nombre de la categoria de los datos a extraer
    Args:
        url: direccion web de los datos a extraer

    Return: nombre de la categoria
    """
    category = url.split('/')[-1].replace('.csv', '')
    logging.info(f'Nombre de la categoria encontrada: {category}')
    return category


def reformat_columns(file_directory):
    """
    Carga el archivo .csv guardado como un Pandas dataframe
    Args: (str) directorio del archivo .csv
    Return: Pandas DataFrame con las columnas formateadas
    """

    data_frame = pd.read_csv(file_directory)

    # Coloca en minusuclas todos los nombres de las columnas
    data_frame.columns = map(str.lower, data_frame.columns)

    # Agrega un guion bajo luego de encontrar 'id'
    data_frame = data_frame.rename(columns=lambda x: re.sub(r'(id)', r'\1_', x))

    # Diccionario para cambiar los nombres de las columnas por los correspondientes
    dictionary = {'cod_loc': 'cod_localidad', 'localid_ad': 'localidad', 'cp': 'código postal',
                  'categoria': 'categoría', 'teléfono': 'número de teléfono', 'telefono': 'número de teléfono',
                  'dirección': 'domicilio', 'direccion': 'domicilio'}

    data_frame = data_frame.rename(columns=dictionary)
    data_frame.to_csv(file_directory, index=False)
    logging.debug(f'Primeras filas del Data Frame {data_frame.head(3)}')


def cines_to_sql(directory):
    """
    Almacena en PostgreSQL la tabla con informacion sobre cine
    Args: (str) directorio donde se almacena la informacion de cine
    """
    columns = ['provincia', 'pantallas', 'butacas', 'espacio_incaa']
    cines = pd.read_csv(f'{directory}', usecols=columns)
    cines['fecha de carga'] = today_str
    cines.to_sql('cines', con=engine, if_exists='replace', index=False)
    logging.debug(f'Cargando cine a PostgreSQL: {cines.head()}')
    logging.info(f'Dimensiones de cine :{cines.shape}')


def all_info_to_sql(dirs):
    """
    Almacena en PostgreSQL una tabla con todos los datos
    Args: (str) directorios donde se almacena los datos a cargar a la base de datos
    """
    temp = []
    columns = ['cod_localidad', 'id_provincia', 'id_departamento', 'categoría', 'provincia', 'localidad', 'nombre',
               'domicilio', 'código postal', 'número de teléfono', 'mail', 'web']

    for path in dirs:
        data_frame = pd.read_csv(path, usecols=columns)
        temp.append(data_frame)

    all_info = pd.concat(temp)
    all_info['fecha de carga'] = today_str
    all_info.to_sql('all_info', con=engine, if_exists='replace', index=False)
    logging.debug(f'Cargando all_info a PostgreSQL: {all_info.head()}')
    logging.info(f'Dimensiones de all_info :{all_info.shape}')


def registers_to_sql(dirs):
    temp = []
    columns = ['categoría', 'fuente', 'provincia']
    for path in dirs:
        data_frame = pd.read_csv(path, usecols=columns)
        temp.append(data_frame)

    info = pd.concat(temp)

    categorias = info.groupby('categoría').size()
    fuentes = info.groupby('fuente').size()
    categoria_provincia = info.groupby(['categoría', 'provincia']).size()

    tabla = pd.concat([categorias, fuentes, categoria_provincia], axis=1)
    columnas = ['Registros por categoria', 'Registros por fuente', 'Registros por categoria y provincia']
    tabla.columns = columnas
    tabla.to_sql('registros', con=engine, if_exists='replace')
    logging.debug(f'Cargando registro a PostgreSQL: {tabla.head(3)}')
    logging.info(f'Dimensiones de resgitro :{tabla.shape}')


def run():
    file_dirs = []

    # Extraccion y formateamos los datos de las urls
    logging.info('-----Request a las URLs-----')
    for link in list_urls:
        file_dir = request_data(link)
        file_dirs.append(file_dir)
        reformat_columns(file_dir)

    # Carga de datos a PostgreSQL
    logging.info('-----Cargando datos a PostgreSQL -----')
    cines_to_sql(file_dirs[1])
    all_info_to_sql(file_dirs)
    registers_to_sql(file_dirs)


if __name__ == '__main__':
    run()
