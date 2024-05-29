from fastapi import FastAPI
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

app = FastAPI()

# -----------------------------------------------------------------------------

df_fun_1_4_5 = pd.read_parquet('Datasets/Procesado/df_fun_1_4_5.parquet')

df_fun_2 = pd.read_parquet('Datasets/Procesado/df_fun_2.parquet')

df_fun_3 = pd.read_parquet('Datasets/Procesado/df_fun_3.parquet')

df_ml = pd.read_parquet('Datasets/Procesado/df_ml.parquet')

# -----------------------------------------------------------------------------

# Endpoint 1


@app.get('/developer')
async def developer(desarrollador: str):

    """Devuelve una lista de la cantidad de ítems y porcentaje de contenido gratuito
        lanzado por año de la desarrolladora ingresada."""

    # Se normaliza el valor convirtiéndolo a minúscula.
    desarrollador_m = desarrollador.lower()

    # Se fijan las filas en las que coincidan el desarrollador correspondiente.
    desarrolladora = df_fun_1_4_5[df_fun_1_4_5['developer'] == desarrollador_m]

    # Se genera una lista para guardar los resultados de las iteraciones.
    resultado = []

    # Se itera sobre los años únicos en los que hay lanzamientos para la desarrolladora.
    for anio in desarrolladora['release_year'].unique():
        # Se filtran los ítems por año de lanzamiento.
        items_anio = desarrolladora[desarrolladora['release_year'] == anio]

        # Se filtran los ítems por año de lanzamiento.
        total_items = items_anio.shape[0]

        # Se cuentan el número de ítems con precio igual a 0.00 en ese año.
        items_gratis = items_anio[items_anio['price'] == 0.00].shape[0]

        # Se calcula el porcentaje de ítems gratuitos
        porcentaje_gratis = (items_gratis / total_items) * 100
    
        # Se agregan los resultados de las iteraciones a la lista
        resultado.append({
            "Año": int(anio),
            "Cantidad de Items": int(total_items),
            "Contenido Free": f"{porcentaje_gratis:.2f}%"
        })

    return resultado

# -----------------------------------------------------------------------------

# Endpoint 2


@app.get('/userdata')
async def userdata(user_id: str):

    """Devuelve la cantidad de dinero gastado, el porcentaje de recomendaciones dadas
        en base al total de juegos que posee y la cantidad total de ítems que posee el usuario ingresado."""

    if type(user_id) != str:
        return 'Error: El valor ingresado debe ser una palabra'

    usuario_m = user_id.lower()

    usuario = df_fun_2[df_fun_2['user_id'] == usuario_m]

    gasto = usuario['price'].sum()

    if usuario['recommend'].sum() == 0:
        porcentaje_recomendacion = 0
    else:
        porcentaje_recomendacion = (usuario['recommend'].sum() / len(usuario)) * 100

    conteo_items = usuario.shape[0]

    datos_usuario = {
        "User X": user_id,
        "Dinero gastado": f"{gasto:.2f} USD",
        "Porcentaje de recomendación": f'{porcentaje_recomendacion}%',
        "Cantidad de items": conteo_items
        }

    return datos_usuario


# -----------------------------------------------------------------------------

# Endpoint 3


@app.get('/user_for_genre')
async def User_For_Genre(genero: str):

    """Devuelve el usuario que mas horas jugó del género ingresado
        y la cantidad de horas acumuladas por año que dicho usuario jugó."""

    genero_m = genero.lower()

    df_genero = df_fun_3[df_fun_3["genres"] == genero_m]

    df_horas_anuales = df_genero.groupby(["release_year"])["playtime_forever"].sum()
    df_horas_anuales = df_horas_anuales.reset_index()

    df_horas = df_genero.groupby("user_id")["playtime_forever"].sum()

    top_horas = df_horas.idxmax()

    df_horas_anuales = df_horas_anuales.rename(columns={"release_year": "Año",  "playtime_forever": "Horas"})
    horas_anuales = df_horas_anuales.to_dict(orient="records")

    return {
            f"Usuario con más horas jugadas para género {genero}": top_horas,
            "Horas jugadas": horas_anuales
            }


# -----------------------------------------------------------------------------

# Endpoint 4


@app.get('/best_developer_year')
async def best_developer_year(anio: int):

    """Devuelve los 3 mejores desarrolladores del año ingresado
        en base a las recomendaciones de los usuarios."""

    if anio not in df_fun_1_4_5['release_year'].unique():
        return f"El año {anio} no existe en los registros."
    juegos_del_año = df_fun_1_4_5[df_fun_1_4_5['release_year'] == anio]

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


# -----------------------------------------------------------------------------

# Endpoint 5

@app.get('/developer_reviews_analysis')
async def developer_reviews_analysis(desarrolladora: str):

    """Devuelve el numero de reseñas negativas y positivas de la desarrolladora ingresada."""


    if type(desarrolladora) != str:
        return 'Error: El valor ingresado debe ser una palabra'

    desarrollador = desarrolladora.lower()

    desarrollador = df_fun_1_4_5[df_fun_1_4_5['developer'] == desarrollador]

    analisis = desarrollador['analisis_sentimiento']
    opinion = analisis.value_counts()

    return {desarrolladora: list([f'Negative: {(opinion.get(0, 0))}',
            f'Positive: {(opinion.get(2, 0))}'])}


# -----------------------------------------------------------------------------


# Sistema de recomendación (Item-Item)


@app.get('/recomendacion_juego')
async def recomendacion_juego(item_id: int):

    vectorizador = TfidfVectorizer()
    matriz = vectorizador.fit_transform(df_ml['combined'])

    coseno = cosine_similarity(matriz, matriz)

    if item_id not in df_ml['item_id'].values:
        return "Error: item_id no encontrado."

    item_index = df_ml[df_ml['item_id'] == item_id].index[0]

    puntuacion_similar = list(enumerate(coseno[item_index]))

    puntuacion_similar = sorted(puntuacion_similar, key=lambda x: x[1], reverse=True)

    puntuacion_similar = puntuacion_similar[1:6]

    item_indices = [i[0] for i in puntuacion_similar]

    item_title = df_ml.loc[df_ml['item_id'] == item_id, 'title'].values[0]

    titulos = [titulo.title() for titulo in df_ml.iloc[item_indices]['title'].tolist()]

    return f"Los títulos similares a '{item_title.title()}' son: {titulos}"
