from fastapi import FastAPI
import pandas as pd

# -------------------------------------------------------------------------------------------------------------------

app = FastAPI()

# -------------------------------------------------------------------------------------------------------------------

df_items = pd.read_parquet('C:\\Users\\fedez\\OneDrive\\Escritorio\\PI-MLOps\\Data\\items.parquet')
df_juegos = pd.read_parquet('C:\\Users\\fedez\\OneDrive\\Escritorio\\PI-MLOps\\Data\\juegos.parquet')
df_resenias = pd.read_parquet('C:\\Users\\fedez\\OneDrive\\Escritorio\\PI-MLOps\\Data\\resenias.parquet')

# -------------------------------------------------------------------------------------------------------------------

# Endpoint 4
@app.get('/developer_reviews_analysis')
async def best_developer_year(anio):

    # Se extraen las columnas de df_resenias necesarias para el merge con df_juegos.
    df_resenias4 = df_resenias[['item_id', 'recommend']]
    df_fun_4 = df_juegos.merge(df_resenias4, on='item_id', how='outer')

    # Se corrobora que el input sea correcto.
    if anio not in df_fun_4['release_year'].unique():
        return f"El año {anio} no existe en los registros."
    
    # Filtrar el dataset para obtener solo las filas correspondientes al año dado.
    juegos_del_año = df_fun_4[df_fun_4['release_year'] == anio]

    # Se calculan las reseñas por desarrolladora.
    resenias = juegos_del_año.groupby('developer')['recommend'].sum().reset_index()

    # Se ordenan los juegos mejor valorados en orden ascendente.
    desarrolladoras = resenias.sort_values(by='recommend', ascending=False)

    # Se ordenan los primeros tres puestos
    oro = desarrolladoras.iloc[0]['developer']
    plata = desarrolladoras.iloc[1]['developer']
    bronce = desarrolladoras.iloc[2]['developer']

    # Crear el diccionario con los tres primeros lugares
    top3 = {"Puesto 1": oro, "Puesto 2": plata,"Puesto 3": bronce}
    return top3

# -------------------------------------------------------------------------------------------------------------------

