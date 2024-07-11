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
query = import_query("features_boleanas.sql")

def ingest_date(query):

    # Executa e trás o resultado para o Python
    df = pd.read_sql(query, ORIGIN_ENGINE)

    # Delete os dados com a data de referência para garantir integridade
    with TARGET_ENGINE.connect() as con:
        try:
            state = f"DELETE FROM results_history"    
            con.execute(sqlalchemy.text(state))
            con.commit()
        except exc.OperationalError as err:
            print("Tabela ainda não existe, criando ela...")

    # Enviando os dados para o novo database
    df.to_sql('results_history', TARGET_ENGINE, index=False, if_exists='replace')

ingest_date(query)

# %% 
query = import_query("features_boleanas.sql")

def ingest_date(query):
    df = pd.read_sql(query, ORIGIN_ENGINE)

    with TARGET_ENGINE.connect() as con:
        try:
            # Use text() para encapsular o comando SQL
            sql = sqlalchemy.text("DROP TABLE IF EXISTS results_history")
            
            # Execute o comando SQL
            con.execute(sql)
            
            # Crie a tabela
            df.head(0).to_sql('results_history', TARGET_ENGINE, index=False)  # Cria a estrutura vazia
            
            # Inserir os dados
            df.to_sql('results_history', TARGET_ENGINE, index=False, if_exists='replace')
            
            print("Tabela criada e dados inseridos com sucesso.")
            
        except Exception as e:
            print(f"Erro ao criar ou inserir dados na tabela: {e}")

ingest_date(query)

