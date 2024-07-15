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
with open('abt2.sql', 'r') as open_file:
    query = open_file.read()

#Processando e trazendo os dados, para um dataframe
df = pd.read_sql(query, engine)
#df = df.drop(['Position', 'Points', 'Position'], axis = 1)
def convert_time_to_seconds(time_str):
    if isinstance(time_str, str) and ':' in time_str:
        minutes, seconds = time_str.split(':')
        return float(minutes) * 60 + float(seconds)
    return float(0)  # Retorna NaN para entradas inválidas

# Aplica a conversão nas colunas Q1, Q2, Q3
df[['Q1', 'Q2', 'Q3']] = df[['Q1', 'Q2', 'Q3']].applymap(convert_time_to_seconds)
df
columns_to_fill = ['Code', 'avg_Q1_time', 'avg_Q2_time', 'avg_Q3_time', 
                    'driver_avg_position', 'driver_total_races']

df[columns_to_fill] = df[columns_to_fill].fillna(0)
df['Code'] = df['Code'].astype(str)
df
# %%
df_train = df[df['Season'] < 2022]
df_test_final = df[df['Season'] >= 2022]

target = 'Position'
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
grid = GridSearchCV(model, param_grid=params, cv=3, scoring='accuracy', n_jobs=-1, verbose=5)

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

from sklearn import metrics
import numpy as np

def report_metrics(y_true, y_proba):
    if y_proba.ndim == 1:
        y_proba = np.vstack([1 - y_proba, y_proba]).T  # Para problemas binários
    
    y_pred = y_proba.argmax(axis=1)  # Para múltiplas classes

    acc = metrics.accuracy_score(y_true, y_pred)
    auc = metrics.roc_auc_score(y_true, y_proba, multi_class='ovr')
    precision = metrics.precision_score(y_true, y_pred, average='weighted')
    recall = metrics.recall_score(y_true, y_pred, average='weighted')

    res = {
        "Acurácia": acc,
        "Curva Roc": auc,
        "Precisão": precision,
        "Recall": recall,
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

model_series = pd.Series({
    'model':model_pipeline,
    'features':features,
    'metrics':df_metrics,
    'dt_train':datetime.datetime.now()
})

model_series.to_pickle('../../models/first_RF.pkl')

# %% 

y_train_proba.shape