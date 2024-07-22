Projeto em andamento com dados da Formula 1

Possíveis Features 
*  Circuito
    - circuitId
    - country

*  Construtor (v)
    - constructorId
    - Nationality
    - quantidade de vitórias por ano
    - média de vitórias por temporada
    - total de vitórias histórica
    - quantidade de vitória por circuito !!

*  Pilotos
    - driverId
    - Nationality
    - Quantidade de vitórias por ano
    - média de vitórias por temporada
    - total de vitórias na carreira 

*  Qualificação
    - Quantidade de poles na carreira
    - Quantidade de poles na temporada
    - Média de poles na carreira
    - quantidade de poles em cada circuito
    - Média de classificação na carreira
    - Média de classificação em cada pista

*  Corrida
    - Total de vitórias por circuito
    - nacionalidade do piloto é a mesma no circuito ?
    - média de posição por cicuito
    - nacionalidade do contrutor é o mesmo do circuito ?
    - posição de largada no grid
    - AverageSpeed, por circuito
    - (diferença entre a posição de largada (Grid) e de chegada (Position)) e por cada circuito
    - piloto venceu as última 1/2/3/4/5/6 corridas 
    - piloto tem vitória nas últimas 5/10 corridas 
    - piloto tem top 3/5 nas últimas 5/10 corridas


OneHotEncoder(drop='first', handle_unknown='ignore')
Fitting 3 folds for each of 3360 candidates, totalling 10080 fits
Best parameters: {'criterion': 'entropy', 'max_depth': 20, 'max_features': 'sqrt', 'max_leaf_nodes': 105, 'min_samples_leaf': 1, 'n_estimators': 50}
Best cross-validation score: 0.4377964354102224


```    
	Acurácia	Curva Roc	Precisão	Recall	    	base
0	0.973050	0.722953	0.960784	0.446809	Train
1	0.962729	0.615252	0.904762	0.231707	Test
2	0.965759	0.657895	1.000000	0.315789	Oot

```
