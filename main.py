from fastapi import FastAPI
import pandas as pd

#-------------------------------------------------------------------------------------------------------------------

app = FastAPI()


df_fun_2 = pd.read_parquet('Datasets/Procesado/df_fun_2.parquet')

df_fun_3 = pd.read_parquet('Data/Procesada/df_fun_3.parquet')

df_fun_4_5 = pd.read_parquet('Datasets/Procesado/df_fun_4_5.parquet')


#-------------------------------------------------------------------------------------------------------------------

# Endpoint 2

@app.get('/userdata')
async def userdata(user_id:str):

    df_sample_fun_2 = df_fun_2.sample(frac=0.1, random_state=50)

    if type(user_id) != str:
        return 'Error: El valor ingresado debe ser una palabra'

    usuario = user_id.lower()

    resenias_usuario = df_sample_fun_2[(df_sample_fun_2['user_id'] == user_id) & (df_sample_fun_2['item_id'].isin(df_sample_fun_2['item_id']))]

    gasto = resenias_usuario['price'].sum()

    if resenias_usuario['recommend'].sum() == 0:
        porcentaje_recomendacion = 0
    else:
        porcentaje_recomendacion = (resenias_usuario['recommend'].sum() / len(resenias_usuario)) * 100

    conteo = df_sample_fun_2[df_sample_fun_2['user_id'] == usuario]
    conteo_items = conteo.shape[0]

    datos_usuario = {
        "User X": user_id,
        "Dinero gastado": f"{gasto:.2f} USD",
        "Porcentaje de recomendación": f'{porcentaje_recomendacion}%',
        "Cantidad de items": conteo_items
        }
    
    return datos_usuario

#-------------------------------------------------------------------------------------------------------------------

# Endpoint 3

@app.get('/user_for_genre')
async def User_For_Genre(genero:str):

    ''' Por cuestion de uso de memoria en Render se procede a tomar una muestra
    del dataframe para evitar complicaciones a la hora de hacer el deploy.'''
    df_sample_fun_3 = df_fun_3.sample(frac=0.1, random_state=50)

    genero_m = genero.lower()
    

    df_genero = df_sample_fun_3[df_sample_fun_3["genres"] == genero_m]

    df_horas_anuales = df_genero.groupby(["release_year"])["playtime_forever"].sum()
    df_horas_anuales = df_horas_anuales.reset_index()

    df_horas = df_genero.groupby("user_id")["playtime_forever"].sum()
    
    top_horas = df_horas.idxmax()

    df_horas_anuales = df_horas_anuales.rename(columns={"release_year": "Año", "playtime_forever": "Horas"})
    horas_anuales = df_horas_anuales.to_dict(orient="records")

    return {
            f"Usuario con más horas jugadas para género {genero}": top_horas,
            "Horas jugadas": horas_anuales
            }

#-------------------------------------------------------------------------------------------------------------------

# Endpoint 4

@app.get('/best_developer_year')
async def best_developer_year(anio:int):

    if anio not in df_fun_4_5['release_year'].unique():
        return f"El año {anio} no existe en los registros."
    juegos_del_año = df_fun_4_5[df_fun_4_5['release_year'] == anio]
    
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
async def developer_reviews_analysis(desarrolladora:str):

    if type(desarrolladora) != str:
        return 'Error: El valor ingresado debe ser una palabra'
    
    desarrollador = desarrolladora.lower()

    desarrollador = df_fun_4_5[df_fun_4_5['developer'] == desarrollador]
    
    analisis = desarrollador['analisis_sentimiento']
    opinion = analisis.value_counts()

    return {desarrolladora: list([f'Negative: {(opinion.get(0, 0))}', f'Positive: {(opinion.get(2, 0))}'])}


