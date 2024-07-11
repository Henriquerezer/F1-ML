# %% 
import pandas as pd
import sqlalchemy
import numpy as np

# %%
engine = sqlalchemy.create_engine("sqlite:///../../data/FEATURES.db")
# Importando a query do sql
with open('abt.sql', 'r') as open_file:
    query = open_file.read()
#
#Processando e trazendo os dados, para um dataframe
df = pd.read_sql(query, engine)
df

# %%
