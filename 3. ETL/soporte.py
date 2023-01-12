import pandas as pd
import requests
from datetime import datetime, timedelta
import ast

# Creamos la clase Extracción
class Extraccion: 
    # definimos el método constructor con las variables globales que usaremos.
    def __init__(self, diccionario_paises, dataframe_ataques): # Recibe un diccionario con los países y coordenadas, el dataframe del archivo araques de tiburones y en formato string el nombre de la columna "layer"
        
        self.diccionario_paises = diccionario_paises
        self.dataframe_ataques = dataframe_ataques
        self.columna = "layer"
        
    # El método llamará a la API para cada país deseado

    def llamada_api (self, dataframe_vacio, producto): # recibe un dataframe vacío y el nombre del producto a solicitar a la api

        # Definimos la variable 
        self.dataframe_vacio = dataframe_vacio
        self.producto = producto

        # hacemos la llamada  a la API iterando por cada uno de los países
        for k,v in self.diccionario_paises.items():
            lon = (v[0])
            lat = (v[1])

            url = f'http://www.7timer.info/bin/api.pl?lon=-{lon}&lat={lat}&product={producto}&output=json'
        
        response = requests.get(url=url)
        
        codigo_estado = response.status_code
        
        razon_estado = response.reason
        
        if codigo_estado == 200:
            print('La peticion se ha realizado correctamente, se ha devuelto el código de estado:',codigo_estado,' y como razón del código de estado: ',razon_estado)
        elif codigo_estado == 402:
            print('No se ha podido autorizar usario, se ha devuelto el código de estado:', codigo_estado,' y como razón del código de estado: ',razon_estado)
        elif codigo_estado == 404:
            print('Algo ha salido mal, el recurso no se ha encontrado,se ha devuelto el código de estado:', codigo_estado,' y como razón del código de estado: ',razon_estado)
        else:
            print('Algo inesperado ha ocurrido, se ha devuelto el código de estado:', codigo_estado,' y como razón del código de estado: ',razon_estado)
        
        response.json()
        # Se convierten los datos obtenidos en un df
        df = pd.DataFrame.from_dict(pd.json_normalize(response.json()['dataseries']))
        
        # Creamos una columna llamada país donde se vaya registrando cada país
        df["country"] = k

        # Concatenamos los df que hemos obtenido con cada país
        df_final = pd.concat([self.dataframe_vacio,df],axis=0, ignore_index= True)

        # Se guarda el df obtenido con la información de la API
        df_final.to_csv("files/3.clima.csv")

        return df_final

    # Este método filtra el df sobre ataques de tiburones a los países deseados
    def filtrar_por_paises (self): #no recibe parámetro
        
        df_ataques_tib = self.dataframe_ataques[(self.dataframe_ataques["country"]== 'usa') | 
            (self.dataframe_ataques["country"]== "australia") | 
            (self.dataframe_ataques["country"]== "new zealand") |
            (self.dataframe_ataques["country"]== "south africa") |
            (self.dataframe_ataques["country"]== "papua new guinea")]
    
        return df_ataques_tib

    # Este método limpia la columna 'wind_profile' del dataframe obtenido a través de la API
    def limpiar_columnas_wind (self, nombre_archivo_wind):

        self.nombre_archivo_wind = nombre_archivo_wind

        # Se abre el archivo que contiene la información obtenida de la API
        df3 = pd.read_csv(f"files/{self.nombre_archivo_wind}")

        df3.reset_index()

        df3['wind_profile']= df3['wind_profile'].apply(ast.literal_eval)

        z = df3['wind_profile'].apply(pd.Series)
        z.head()

        # Por eso empezamos con un for para iterar por cada una de las columnas. 
        for i in range(len(z.columns)): 

            # aplicamos el apply,extraemos el valore de la key "layer" y lo almacenamos en una variable que convertimos a string 
            nombre = "wind_dir_" + str(z[i].apply(pd.Series)[self.columna][0]) 

            # hacemos lo mismo con una variable que se llame valores para "guardar" los valores de la celda
            valores = list(z[i].apply(pd.Series)['direction'])

            # usamos el método insert de los dataframes para ir añadiendo esta información a el dataframe con la información del clima. 
            df3.insert(i, nombre, valores)

        df3.to_csv("files/4.clima_wind.csv")

        return df3

    # Este método limpia la columna 'rh_profile' del dataframe obtenido a través de la API
    def limpiar_columnas_rh (self,nombre_archivo_rh):
        
        self.nombre_archivo_rh = nombre_archivo_rh

        # Se abre el archivo creado con la limpieza de la columna wind_profile
        df4 = pd.read_csv(self.nombre_archivo_rh)

        df4.reset_index()

        df4['rh_profile']= df4['rh_profile'].apply(ast.literal_eval)

        z = df4['rh_profile'].apply(pd.Series)
        z.head()

        # Itera por cada una de las columnas. 
        for i in range(len(z.columns)): 

            # aplicamos el apply,extraemos el valore de la key "layer" y lo almacenamos en una variable que convertimos a string 
            nombre = "rh_" + str(z[i].apply(pd.Series)[self.columna][0]) 

            # hacemos lo mismo con una variable que se llame valores para "guardar" los valores de la celda
            valores = list(z[i].apply(pd.Series)["rh"] )

            # usamos el método insert de los dataframes para ir añadiendo esta información a el dataframe con la información del clima. 
            df4.insert(i, nombre, valores)

            # Guardamos el dataframe obtenido con la limpieza de las columnas.
            df4.to_csv("files/5.clima_wind_rh.csv")

        return df4
