# %% 
import pandas as pd 
import sqlalchemy

# %%
engine = sqlalchemy.create_engine("sqlite:///../../data/FEATURES.db")
# Importando a query do sql
with open('etl.sql', 'r') as open_file:
    query = open_file.read()

#Processando e trazendo os dados, para um dataframe
df = pd.read_sql(query, engine)
df = df.drop(['Position', 'Points', 'Position'], axis = 1)
df['AverageSpeed'] = df['AverageSpeed'].astype(float)
df_history = df
df_history

# %%
df_history['country'].unique()
df_history[df_history['country'] == 'Hungary'].CircuitID.unique()


# %%
circuito_atual = 'hungaroring'
df_avg_speed = df_history[df_history['CircuitID'] == circuito_atual].groupby('DriverID')['AverageSpeed'].mean().reset_index()
df_avg_speed.columns = ['DriverID', 'AverageSpeedMean']

# Mesclar a média de AverageSpeed com os novos dados
df = df.merge(df_avg_speed, on='DriverID', how='left')

# Substituir valores NaN por 0 na média de velocidade (caso não haja histórico)
df['AverageSpeedMean'] = df['AverageSpeedMean'].fillna(0)

# Exibir as primeiras linhas do DataFrame transformado
df[(df['CircuitID'] == 'hungaroring') & (df['DriverID'] == 'max_verstappen')][['Season','CircuitID','DriverID','AverageSpeed','AverageSpeedMean']]