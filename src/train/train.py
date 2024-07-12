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
    'min_samples_leaf': [3, 5, 10, 25],
    'n_estimators': [100, 150 ,200, 500],
    'criterion': ["gini", 'entropy'],
    'max_depth': [18, 20, 25, 30, 35, 40 ],
    'max_features' : ['sqrt', 'log2'],
    'max_leaf_nodes':[50, 55, 75, 85, 95]
}


# Configuração do GridSearchCV
grid = GridSearchCV(model, param_grid=params, cv=3, scoring='f1', n_jobs=-1, verbose=5)

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
#y_test_proba  = model_pipeline.predict_proba(X_test)

print('acc', metrics.accuracy_score(y_train, y_train_proba))
print('auc' ,metrics.roc_auc_score(y_train, y_train_proba))
print('precision' , metrics.precision_score(y_train, y_train_proba))
print('recall' ,metrics.recall_score(y_train, y_train_proba))

# %%

#y_train_proba = model_pipeline.predict(X_train)
y_test_proba  = model_pipeline.predict(X_test)

print('acc', metrics.accuracy_score(y_test, y_test_proba))
print('auc' ,metrics.roc_auc_score(y_test, y_test_proba))
print('precision' , metrics.precision_score(y_test, y_test_proba))
print('recall' ,metrics.recall_score(y_test, y_test_proba))

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

df_metrics = pd.DataFrame([report_train,report_test])
df_metrics