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

    df_resenias4 = df_resenias[['item_id', 'recommend']]
    df_juegos4 = df_juegos[['item_id', 'developer', 'release_year']]
    df_fun_4 = df_juegos4.merge(df_resenias4, on='item_id', how='outer') 

    if anio not in df_fun_4['release_year'].unique():
        return f"El año {anio} no existe en los registros."
    juegos_del_año = df_fun_4[df_fun_4['release_year'] == anio]
    
    resenias = juegos_del_año.groupby('developer')['recommend'].sum().reset_index()

    desarrolladoras = resenias.sort_values(by='recommend', ascending=False)

    oro = desarrolladoras.iloc[0]['developer']
    plata = desarrolladoras.iloc[1]['developer']
    bronce = desarrolladoras.iloc[2]['developer']

    top3 = {"Puesto 1": oro.title(), "Puesto 2": plata.title(), "Puesto 3": bronce.title()}

    return top3

# -------------------------------------------------------------------------------------------------------------------

# Endpoint 5

def developer_reviews_analysis(desarrolladora):

    df_juegos5 = df_juegos[['item_id', 'developer']]
    df_resenias5 = df_resenias[['item_id', 'analisis_sentimiento']]
    df_fun_5 = df_resenias5.merge(df_juegos5, on='item_id', how='outer')

    if type(desarrolladora) != str:
        return 'Error: El valor ingresado debe ser una palabra'
    
    desarrollador = desarrolladora.lower()

    desarrollador = df_fun_5[df_fun_5['developer'] == desarrollador]
    
    analisis = desarrollador['analisis_sentimiento']
    opinion = analisis.value_counts()

    return {desarrolladora : list([f'Negative: {(opinion.get(0, 0))}', f'Positive: {(opinion.get(2, 0))}'])}
