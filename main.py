from fastapi import FastAPI, HTTPException
import pandas as pd
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

# -------------------------------------------------------------------------------------------------------------------

app = FastAPI()

# -------------------------------------------------------------------------------------------------------------------

# Endpoint 4
@app.get('/best_developer_year')
async def best_developer_year(anio:int):

    df_fun_4 = pd.read_parquet('C:\Users\fedez\OneDrive\Escritorio\PI-MLOps\Data\Procesada\df_fun_4_5.parquet')

    if anio not in df_fun_4['release_year'].unique():
        return f"El año {anio} no existe en los registros."
    juegos_del_año = df_fun_4[df_fun_4['release_year'] == anio]
    
    resenias = juegos_del_año.groupby('developer')['recommend'].sum().reset_index()

    desarrolladoras = resenias.sort_values(by='recommend', ascending=False)

    oro = desarrolladoras.iloc[0]['developer']
    plata = desarrolladoras.iloc[1]['developer']
    bronce = desarrolladoras.iloc[2]['developer']

    top3 = {
            "Puesto 1": oro.title(),
            "Puesto 2": plata.title(),
            "Puesto 3": bronce.title()
            }

    return top3

# -------------------------------------------------------------------------------------------------------------------

# Endpoint 5

@app.get('/developer_reviews_analysis')
async def developer_reviews_analysis(desarrolladora):

    df_fun_5 = pd.read_parquet('C:\Users\fedez\OneDrive\Escritorio\PI-MLOps\Data\Procesada\df_fun_4_5.parquet')

    if type(desarrolladora) != str:
        return 'Error: El valor ingresado debe ser una palabra'
    
    desarrollador = desarrolladora.lower()

    desarrollador = df_fun_5[df_fun_5['developer'] == desarrollador]
    
    analisis = desarrollador['analisis_sentimiento']
    opinion = analisis.value_counts()

    return {desarrolladora : list([f'Negative: {(opinion.get(0, 0))}', f'Positive: {(opinion.get(2, 0))}'])}
