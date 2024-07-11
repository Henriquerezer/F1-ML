# %% 
import pandas as pd
import sqlalchemy
import numpy as np

from sklearn import model_selection

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
# fazer dummie das cat e normalizar as numéricas