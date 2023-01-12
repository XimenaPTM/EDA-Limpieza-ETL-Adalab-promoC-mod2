import pandas as pd
import soporte as sp

df = pd.read_csv("files/1.paises_meteo_info.csv", index_col = 0)
print(df.head(2))

df2 = pd.read_csv("datos/attacks_limpieza_completa.csv", index_col =0)
print(df2.head(2))

dic_paises = {"USA":[-100.445882,39.7837304,], 
            "Australia":[134.755,-24.7761086],
            "South_Africa":[24.991639,-28.8166236],
            "New Zealand":[172.8344077,-41.5000831],
            "Papua New Guinea":[144.2489081,-5.6816069]}
producto_meteo = "meteo"
df_empty = pd.DataFrame()
name_file_wind = "3.clima.csv"
name_file_rh = "files/4.clima_wind.csv"


# Creamos una variable llamada api que es resultado de la aplicación de la clase Extracción
# Recibe como argumentos latitud, longitud y ciudad (Variables definidas anteriormente).
print("Llamando a la clase Extracción")
api = sp.Extraccion(dic_paises,df2)
print(api)
print("--------------------------")

# Definida la clase, se le aplica el método llamada_api
print("Aplicando el método llamada_apy")
df_country = api.llamada_api (df_empty, producto_meteo)
print(df_country.head(2))
print("--------------------------")

# Definida la clase, se le aplica el método filtrar 
print("Aplicando el método filtrar por paises")
df_ataques_filtrado = api.filtrar_por_paises()
print("df_country")
print(df_country.head(2))
print("--------------------------")

# Definida la clase, se le aplica el método filtrar 
print("Aplicando el método limpiar columnas wind")
df_wind_limpio = api.limpiar_columnas_wind (name_file_wind)
print("df_wind_limpio")
print(df_wind_limpio.head(2))
print("--------------------------")