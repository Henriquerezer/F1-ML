# %% 
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
df = df.drop(['Position', 'Points', 'Position','DifferenceGridPosition'], axis = 1)
df['AverageSpeed'] = df['AverageSpeed'].astype(float)
df


# %%

# TESTANDO COM APENAS 1 CORRIDA 
target = 'win'
df_test_final1 = df[(df['Season'] == 2024) & (df['Round'] == 10)]

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
y_oot_proba   = model_series['model'].predict(df_test_final1[model_series['features']])

# Relatar métricas e plotar gráficos para o conjunto OOT (Out-of-Time)
oot_metrics = report_metrics(df_test_final1[target], y_oot_proba)
print("Métricas OOT:", oot_metrics)