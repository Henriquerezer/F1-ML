# %% 
import matplotlib.pyplot as plt
from sklearn import metrics
import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy import exc

engine = sqlalchemy.create_engine("sqlite:///../../data/FEATURES.db")
# Importando a query do sql
with open('etl.sql', 'r') as open_file:
    query = open_file.read()

model_series = pd.read_pickle('../../models/first_RF.pkl')
model_series

#Processando e trazendo os dados, para um dataframe
df = pd.read_sql(query, engine)
df = df.drop(['Position', 'Points'], axis = 1)
df['AverageSpeed'] = df['AverageSpeed'].astype(float)
df



# %%
# TESTANDO COM MAIS DE 1 CORRIDA 
df_test_final1 = df[(df['Season'] == 2024) & (df['Round'] >= 16)]

# DataFrame para armazenar os resultados finais
results = []

# Obter os rounds únicos a partir do df_test_final1
unique_rounds = df_test_final1['Round'].unique()

# Iterar sobre cada round
for round_number in unique_rounds:
    # Filtrar os dados para o round atual
    df_round = df_test_final1[df_test_final1['Round'] == round_number].copy()
    
    # Prever as probabilidades para o round atual
    y_oot_proba = model_series['model'].predict_proba(df_round[model_series['features']])
    
    # Selecionar a coluna que representa a probabilidade da classe 1 (vencedor)
    prob_victory = y_oot_proba[:, 1]
    
    # Imprimir as probabilidades de vitória de cada piloto
    print(f"Round {round_number} - Probabilidades de vitória:")
    for i, prob in enumerate(prob_victory):
        print(f"Piloto {i}: Probabilidade de vitória = {prob:.4f}")
    
    # Criar um array de zeros com o mesmo tamanho que prob_victory
    predicted_winner = np.zeros_like(prob_victory, dtype=int)
    
    # Encontrar o índice do piloto com a maior probabilidade de vitória
    winner_index = np.argmax(prob_victory)
    
    # Definir esse índice como 1 (vencedor)
    predicted_winner[winner_index] = 1
    
    # Adicionar a coluna de probabilidade e a predição ao DataFrame do round atual usando .loc
    df_round.loc[:, 'prob_victory'] = prob_victory
    df_round.loc[:, 'predicted_winner'] = predicted_winner
    
    # Adicionar os resultados do round atual à lista de resultados
    results.append(df_round)

# Concatenar todos os resultados em um único DataFrame
df_final_results = pd.concat(results)

# Exibir os resultados finais
print("\nResultados Finais:")

# %%
# DATAFRAME FINAL
df_final_results[['Season','Round','CircuitID','country','DriverID','predicted_winner','prob_victory']]



