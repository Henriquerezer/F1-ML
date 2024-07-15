# %%
import pandas as pd
import sqlalchemy
from sqlalchemy import exc
from tqdm import tqdm

# %%
def import_query(path):
    with open(path, 'r') as open_file:
        return open_file.read()

ORIGIN_ENGINE = sqlalchemy.create_engine("sqlite:///../../data/F1_DATA.db")
TARGET_ENGINE = sqlalchemy.create_engine("sqlite:///../../data/FEATURES.db")

# %% 
query = import_query("features_qualy.sql")

def ingest_date(query):
    df = pd.read_sql(query, ORIGIN_ENGINE)

    with TARGET_ENGINE.connect() as con:
        try:
            # Use text() para encapsular o comando SQL
            sql = sqlalchemy.text("DROP TABLE IF EXISTS results_qualy")
            
            # Execute o comando SQL
            con.execute(sql)
            
            # Crie a tabela
            df.head(0).to_sql('results_qualy', TARGET_ENGINE, index=False)  # Cria a estrutura vazia
            
            # Inserir os dados
            df.to_sql('results_qualy', TARGET_ENGINE, index=False, if_exists='replace')
            
            print("Tabela criada e dados inseridos com sucesso.")
            
        except Exception as e:
            print(f"Erro ao criar ou inserir dados na tabela: {e}")

ingest_date(query)
