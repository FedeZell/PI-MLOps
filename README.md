# <center>Proyecto Individual N° 1 - Machine Learning Operations</center>
<p align="center">
<img src="https://user-images.githubusercontent.com/67664604/217914153-1eb00e25-ac08-4dfa-aaf8-53c09038f082.png"  height=300>
</p>



## **Descripción del proyecto**
El proyecto consiste en la implementación de un framework FastAPI para el desarrollo de 5 endpoints y un modelo de aprendizaje automático.


## **Endpoints propuestos**

+ def **developer( *desarrollador : str* )**:
    Cantidad de items y porcentaje de contenido Free por año según empresa desarrolladora.
  
Ejemplo de retorno:
| Año  | Cantidad de Items | Contenido Free  |
|------|-------------------|------------------|
| 2023 | 50                | 27%              |
| 2022 | 45                | 25%              |
| xxxx | xx                | xx%              |


+ def **userdata( *User_id : str* )**:
    Debe devolver cantidad de dinero gastado por el usuario, el porcentaje de recomendación en base a reviews.recommend y cantidad de items.
Ejemplo de retorno: {"Usuario X" : us213ndjss09sdf, "Dinero gastado": 200 USD, "% de recomendación": 20%, "cantidad de items": 5}

+ def **UserForGenre( *genero : str* )**:
    Debe devolver el usuario que acumula más horas jugadas para el género dado y una lista de la acumulación de horas jugadas por año de lanzamiento.
Ejemplo de retorno: {"Usuario con más horas jugadas para Género X" : us213ndjss09sdf,
			     "Horas jugadas":[{Año: 2013, Horas: 203}, {Año: 2012, Horas: 100}, {Año: 2011, Horas: 23}]}
	
+ def **best_developer_year( *año : int* )**:
   Devuelve el top 3 de desarrolladores con juegos MÁS recomendados por usuarios para el año dado. (reviews.recommend = True y comentarios positivos)
Ejemplo de retorno: [{"Puesto 1" : X}, {"Puesto 2" : Y},{"Puesto 3" : Z}]

+ def **developer_reviews_analysis( *desarrolladora : str* )**:
    Según el desarrollador, se devuelve un diccionario con el nombre del desarrollador como llave y una lista con la cantidad total 
    de registros de reseñas de usuarios que se encuentren categorizados con un análisis de sentimiento como valor positivo o negativo. 
Ejemplo de retorno: {'Valve' : [Negative = 182, Positive = 278]}

## **Modelo de aprendizaje automático**

[API](https://pi-mlops-djwn.onrender.com/docs)

# _Autor_
- Federico Germán Zellweger Semino
- E-mail: fedezell97@gmail.com
