# %% 
import pandas as pd
import sqlalchemy
import numpy as np
from sklearn import pipeline
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn import model_selection
from sklearn import metrics
import datetime
# %%
engine = sqlalchemy.create_engine("sqlite:///../../data/FEATURES.db")
# Importando a query do sql
with open('abt.sql', 'r') as open_file:
    query = open_file.read()

#Processando e trazendo os dados, para um dataframe
df = pd.read_sql(query, engine)
df = df.drop(['Position', 'Points'], axis = 1)
df['AverageSpeed'] = df['AverageSpeed'].astype(float)
df
# %%
df_train = df[df['Season'] < 2022]
df_test_final = df[df['Season'] >= 2022]

target = 'win'
features = df_train.columns[2:-1].to_list()
#%% 
X_train, X_test, y_train, y_test = model_selection.train_test_split(df_train[features],
                                                                    df_train[target],
                                                                    random_state=42,
                                                                    train_size=0.8,
                                                                    stratify=df_train[target])

print("Taxa de resposta na base de Train", y_train.mean())
print("Taxa de resposta na base de Test", y_test.mean())

# %% 
categorical_columns = X_train.select_dtypes(include=['object', 'category']).columns.to_list()

# Selecionar colunas numéricas
numeric_columns = X_train.select_dtypes(include=['number']).columns.to_list()

print("Colunas Categóricas:")
print(categorical_columns)

print("\nColunas Numéricas:")
print(numeric_columns)

# %%
# Dummie das cat e normalizar as numéricas

# Criar transformadores
categorical_transformer = OneHotEncoder(drop='first', handle_unknown='ignore')  # drop='first' para evitar multicolinearidade
numeric_transformer = StandardScaler()

# Criar o ColumnTransformer
preprocessor = ColumnTransformer(
    transformers=[
        ('num', numeric_transformer, numeric_columns),
        ('cat', categorical_transformer, categorical_columns)
    ])

# %%
model = RandomForestClassifier(random_state=42)

# Definição dos hiperparâmetros para o GridSearchCV
params = {
    'min_samples_leaf': [1,2,3, 5,],
    'n_estimators': [50,100, 150 ,200, 500, 600, 700],
    'criterion': ["gini", 'entropy'],
    'max_depth': [18, 20, 22, 25, 30,],
    'max_features' : ['sqrt', 'log2'],
    'max_leaf_nodes':[75, 80, 85, 95, 100, 105]
}


# Configuração do GridSearchCV
grid = GridSearchCV(model, param_grid=params, cv=3, scoring='f1', n_jobs=-1, verbose=10)

# Criação do pipeline que inclui o pré-processamento e o modelo
model_pipeline = pipeline.Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('classifier', grid)
])

# Ajuste do pipeline aos dados de treino
model_pipeline.fit(X_train, y_train)

# Avaliação nos dados de teste
print(f"Best parameters: {grid.best_params_}")
print(f"Best cross-validation score: {grid.best_score_}")


# %%

y_train_proba = model_pipeline.predict(X_train)
y_test_proba  = model_pipeline.predict(X_test)
y_oot_proba   = model_pipeline.predict(df_test_final[features])

# %%

def report_metrics(y_true, y_proba):
    y_pred = (y_proba).astype(int)

    acc = metrics.accuracy_score(y_true, y_pred)
    auc = metrics.roc_auc_score(y_true, y_proba)
    precision = metrics.precision_score(y_true, y_pred)
    recall = metrics.recall_score(y_true, y_pred)

    res = {
        "Acurácia" : acc,
        "Curva Roc" : auc,
        "Precisão" : precision,
        "Recall" : recall,
    }
    return res

report_train = report_metrics(y_train, y_train_proba)
report_train['base'] = 'Train'
report_test = report_metrics(y_test, y_test_proba)
report_test['base'] = 'Test'
report_oot = report_metrics(df_test_final[target], y_oot_proba)
report_oot['base'] = 'Oot'

df_metrics = pd.DataFrame([report_train,report_test,report_oot])
df_metrics

# %%

# SALVANDO MODELO
model_series = pd.Series({
    'model':model_pipeline,
    'features':features,
    'metrics':df_metrics,
    'dt_train':datetime.datetime.now()
})

model_series.to_pickle('../../models/first_RF.pkl')

# %%

# TESTANDO COM APENAS 1 CORRIDA 

df_test_final1 = df_test_final[(df_test_final['Season'] == 2024) & (df_test_final['Round'] == 12)]

import matplotlib.pyplot as plt
from sklearn import metrics
import numpy as np

# Função para relatar métricas e plotar gráficos
def report_metrics(y_true, y_proba):
    y_pred = (y_proba).astype(int)

    acc = metrics.accuracy_score(y_true, y_pred)
    auc = metrics.roc_auc_score(y_true, y_proba)
    precision = metrics.precision_score(y_true, y_pred)
    recall = metrics.recall_score(y_true, y_pred)
    
    # Matriz de confusão
    confusion_matrix = metrics.confusion_matrix(y_true, y_pred)
    
    res = {
        "Acurácia" : acc,
        "Curva Roc" : auc,
        "Precisão" : precision,
        "Recall" : recall,
    }

    # Plotagem da curva ROC
    fpr, tpr, _ = metrics.roc_curve(y_true, y_proba)
    plt.figure(figsize=(10, 5))
    plt.subplot(1, 2, 1)
    plt.plot(fpr, tpr, color='blue', lw=2, label=f'AUC = {auc:.2f}')
    plt.plot([0, 1], [0, 1], color='gray', lw=2, linestyle='--')
    plt.xlabel('Taxa de Falsos Positivos')
    plt.ylabel('Taxa de Verdadeiros Positivos')
    plt.title('Curva ROC')
    plt.legend(loc='lower right')
    
    # Plotagem da matriz de confusão
    plt.subplot(1, 2, 2)
    metrics.ConfusionMatrixDisplay(confusion_matrix).plot(cmap=plt.cm.Blues, ax=plt.gca())
    plt.title('Matriz de Confusão')
    plt.grid(False)
    plt.show()

    return res

# Previsões
y_train_proba = model_pipeline.predict(X_train)
y_test_proba  = model_pipeline.predict(X_test)
y_oot_proba   = model_pipeline.predict(df_test_final1[features])

# Relatar métricas e plotar gráficos para o conjunto de treinamento
train_metrics = report_metrics(y_train, y_train_proba)
print("Métricas de Treinamento:", train_metrics)

# Relatar métricas e plotar gráficos para o conjunto de teste
test_metrics = report_metrics(y_test, y_test_proba)
print("Métricas de Teste:", test_metrics)

# Relatar métricas e plotar gráficos para o conjunto OOT (Out-of-Time)
oot_metrics = report_metrics(df_test_final1[target], y_oot_proba)
print("Métricas OOT:", oot_metrics)
