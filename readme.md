Projeto em andamento com dados da Formula 1

# Descrição do Projeto
Este projeto utiliza dados históricos da Fórmula 1 para criar e treinar modelos de Machine Learning, a fim de prever resultados de corridas. As features selecionadas para o treinamento do modelo são descritas abaixo.

## Features Selecionadas
As seguintes features foram escolhidas para o treinamento do modelo:

**Season**: Ano da temporada.

**Round**: Número da rodada dentro da temporada.

**CircuitID**: Identificador único do circuito.

**Country**: País onde o circuito está localizado.

**ConstructorName**: Nome da equipe.

**ConstructorID**: Identificador único da equipe.

**ConstructorNationality**: Nacionalidade da equipe.

**DriverID**: Identificador único do piloto.

**DriverName**: Nome do piloto.

**DriverNationality**: Nacionalidade do piloto.

**Grid**: Posição do piloto na largada.
 
**Status**: Status do piloto ao final da corrida.

**AverageSpeed**: Velocidade média do Piloto.

**DifferenceGridPosition**: Difenreça da posição inicial e posição final

**WonLastRace,, WonLast2Races, WonLast3Races, WonLast4Races, WonLast5Races, WonLast6Races, WonLast7Races, WonLast10Races, Top3Last5Races, Top5Last10Races**: Dados do piloto em relação as últimas corridas.


# Treinamento e Avaliação do Modelo
Durante o processo de treinamento, foram utilizadas três bases de dados: base de treino (dividida em treino e validação) e base de teste. A base de teste contém dados que não foram apresentados durante o treinamento.

## Pipeline de Treinamento
Um pipeline foi utilizado para o treinamento, com os seguintes componentes de pré-processamento:

### Transformação Categórica: OneHotEncoder(drop='first', handle_unknown='ignore') para evitar multicolinearidade.
### Transformação Numérica: StandardScaler() para normalização das variáveis numéricas.
O ColumnTransformer foi configurado da seguinte forma:

```
preprocessor = ColumnTransformer(
    transformers=[
        ('num', numeric_transformer, numeric_columns),
        ('cat', categorical_transformer, categorical_columns)
    ])
```

O modelo utilizado foi um **RandomForestClassifier** com otimização de hiperparâmetros através de **GridSearchCV**:

```
params = {
    'min_samples_leaf': [1, 2, 3],
    'n_estimators': [30, 40, 50, 100, 150, 200],
    'criterion': ["gini", 'entropy'],
    'max_depth': [18, 20, 22, 25, 30],
    'max_features' : ['sqrt', 'log2'],
    'max_leaf_nodes': [95, 100, 105, 110, 115, 120]
}
grid = GridSearchCV(model, param_grid=params, cv=3, scoring='f1', n_jobs=-1, verbose=10)
```

O **pipeline completo**, combinando pré-processamento e modelo, foi definido da seguinte maneira:

```
model_pipeline = pipeline.Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('classifier', grid)
])
model_pipeline.fit(X_train, y_train)
```

## Desempenho do Modelo
As métricas de desempenho foram extraídas para cada conjunto de dados (treino, validação e teste):
```
Acurácia	Curva Roc	Precisão	Recall		Base
0.974054	0.733591	0.9625		0.468085	Train
0.963303	0.627146	0.8750		0.256098	Test
0.967213	0.672414	1.0000		0.344828	Oot
```

## Acompanhamento do Modelo
O MLflow foi utilizado para acompanhar o modelo. Inicialmente, apenas o modelo de Random Forest foi monitorado, mas no futuro serão incluídos mais modelos e seus hiperparâmetros serão armazenados utilizando o MLflow.

<div align="center">
<img src="https://github.com/user-attachments/assets/86752da1-5e39-4c03-a37b-60d4cbe2beff" width="600px" />
</div>











--------------------------------------
**Possíveis Features (FUTURAS)**
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


Notas:
 - Atualmente estou usando o status na corrida, mas poderia usar bolenas para o status na últimas corridas ... 
 - O ideal depois, seria trocar a API, para ter mais infos, como condição do tempo, lat e long do circuito,  talvez até infos em tempo real