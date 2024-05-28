from fastapi import FastAPI, HTTPException
import pandas as pd

app = FastAPI()

#-------------------------------------------------------------------------------------------------------------------

df_fun_1 = pd.read_parquet('Datasets/Procesado/df_fun_1.parquet')

df_fun_2 = pd.read_parquet('Datasets/Procesado/df_fun_2.parquet')

df_fun_3 = pd.read_parquet('Datasets/Procesado/df_fun_3.parquet')

df_fun_4_5 = pd.read_parquet('Datasets/Procesado/df_fun_4_5.parquet')

#-------------------------------------------------------------------------------------------------------------------

# Endpoint 1

'''@app.get('/developer')
async def developer(desarrollador:str):

    if type(desarrollador) != str:
        return 'Error: El valor ingresado debe ser una palabra'
    
    desarrollador_m = desarrollador.lower()

    desarrolladora = df_fun_1[df_fun_1['developer'] == desarrollador_m]
    
    result = {}

    for year in desarrolladora['release_year'].unique():

        items_year = desarrolladora[desarrolladora['release_year'] == year]

        total_items = items_year.shape[0]

        free_items = items_year[items_year['price'] == 0.00].shape[0]

        percentage_free_items = (free_items / total_items) * 100
        
        result[year] = {
            "Año": year,
            "Cantidad de Items": total_items,
            "Contenido Free": f"{percentage_free_items:.2f}%"
        }

    return result'''

'''@app.get('/developer')
async def developer(desarrollador: str):
    if type(desarrollador) != str:
        raise HTTPException(status_code=400, detail="El valor ingresado debe ser una palabra")

    desarrollador_m = desarrollador.lower()

    desarrolladora = df_fun_1[df_fun_1['developer'] == desarrollador_m]

    if desarrolladora.empty:
        raise HTTPException(status_code=404, detail="Desarrolladora no encontrada")

    result = []
    
    for year in desarrolladora['release_year'].unique():
        items_year = desarrolladora[desarrolladora['release_year'] == year]
        total_items = items_year.shape[0]
        free_items = items_year[items_year['price'] == 0.00].shape[0]
        percentage_free_items = (free_items / total_items) * 100

        result.append({
            "Año": year,
            "Cantidad de Items": total_items,
            "Contenido Free": f"{percentage_free_items:.2f}%"
        })

    return result'''

@app.get('/developer')
async def developer(desarrollador):
    # Filtrar el DataFrame por el desarrollador proporcionado
    df_desarrollador = df_fun_1[df_fun_1['developer'] == desarrollador]

    # Agrupar por año y calcular la cantidad de items y el porcentaje de contenido gratuito
    stats_por_anio = df_desarrollador.groupby('release_anio').agg({
        'items_free': 'sum',
        'items_total': 'sum',
        'percentage_free': lambda x: (pd.to_numeric(x.str.rstrip('%'), errors='coerce') / 100).mean() * 100
    }).reset_index()

    # Redondear a dos decimales
    stats_por_anio['percentage_free'] = stats_por_anio['percentage_free'].round(2)

    # Cambiar el nombre de la columna
    stats_por_anio = stats_por_anio.rename(columns={'release_anio': 'Año'})
    stats_por_anio = stats_por_anio.rename(columns={'items_total': 'Items'})
    stats_por_anio = stats_por_anio.rename(columns={'percentage_free': '% Free'})

    # Mostrar los resultados
    return stats_por_anio[['Año', 'Items', '% Free']]


#-------------------------------------------------------------------------------------------------------------------
# Endpoint 2

@app.get('/userdata')
async def userdata(user_id: str):

    if type(user_id) != str:
        return 'Error: El valor ingresado debe ser una palabra'

    usuario_m = user_id.lower()

    usuario = df_fun_2[df_fun_2['user_id'] == usuario_m]

    gasto = usuario['price'].sum()

    if usuario['recommend'].sum() == 0:
        porcentaje_recomendacion = 0
    else:
        porcentaje_recomendacion =  (usuario['recommend'].sum() / len(usuario)) * 100

    conteo_items = usuario.shape[0]

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

    genero_m = genero.lower()
    
    df_genero = df_fun_3[df_fun_3["genres"] == genero_m]

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
