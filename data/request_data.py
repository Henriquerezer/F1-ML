# %%

# DADOS DOS PILOTOS USANDO A API ERGAST (NÃO SERÁ MAIS ATUALIZADA AO FINAL DE 2024) 
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy

def fetch_drivers(offset=0):
    url = f"http://ergast.com/api/f1/drivers?offset={offset}"
    response = requests.get(url)
    
    if response.status_code != 200:
        print("Erro ao obter os dados:", response.status_code)
        return []
    
    root = ET.fromstring(response.text)
    drivers = []
    for driver in root.findall(".//{http://ergast.com/mrd/1.5}Driver"):
        driver_data = {
            "driverId": driver.get("driverId"),
            "url": driver.get("url"),
            "GivenName": driver.find("{http://ergast.com/mrd/1.5}GivenName").text,
            "FamilyName": driver.find("{http://ergast.com/mrd/1.5}FamilyName").text,
            "DateOfBirth": driver.find("{http://ergast.com/mrd/1.5}DateOfBirth").text,
            "Nationality": driver.find("{http://ergast.com/mrd/1.5}Nationality").text,
        }
        drivers.append(driver_data)
    
    return drivers

# Inicializa a lista de todos os pilotos
all_drivers = []

# Limite de pilotos por página (30 por padrão, pode ser ajustado pela API)
limit_per_page = 30
offset = 0

while True:
    drivers = fetch_drivers(offset=offset)
    if not drivers:
        break
    all_drivers.extend(drivers)
    offset += limit_per_page

# Cria o DataFrame com todos os dados de pilotos
df = pd.DataFrame(all_drivers)
df = df.sort_values(by=['DateOfBirth'], ascending=False, ignore_index=True).reset_index()

# Cria a engine para o banco de dados SQLite
engine = create_engine('sqlite:///../data/F1_DATA.db')

# Salva o DataFrame em uma tabela chamada "drivers" no banco de dados SQLite
df.to_sql('drivers', con=engine, if_exists='replace', index=False)

print("Dados salvos no banco de dados com sucesso.")


# %%
# DADOS DOS CONSTRUTORES USANDO A API ERGAST (NÃO SERÁ MAIS ATUALIZADA AO FINAL DE 2024) 

import requests
import xml.etree.ElementTree as ET
import pandas as pd
from tqdm import tqdm

def fetch_constructors(year, offset=0):
    url = f"http://ergast.com/api/f1/{year}/constructors?offset={offset}"
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"Erro ao obter os dados do ano {year}:", response.status_code)
        return []
    
    root = ET.fromstring(response.text)
    constructors = []
    for constructor in root.findall(".//{http://ergast.com/mrd/1.5}Constructor"):
        constructor_data = {
            "year": year,
            "constructorId": constructor.get("constructorId"),
            "url": constructor.get("url"),
            "Name": constructor.find("{http://ergast.com/mrd/1.5}Name").text,
            "Nationality": constructor.find("{http://ergast.com/mrd/1.5}Nationality").text,
        }
        constructors.append(constructor_data)
    
    return constructors

# Inicializa a lista de todos os construtores
all_constructors = []

# Define o intervalo de anos
start_year = 1950
end_year = 2024

# Usa tqdm para criar a barra de progresso
for year in tqdm(range(start_year, end_year + 1), desc="Fetching Constructors Data"):
    offset = 0
    while True:
        constructors = fetch_constructors(year, offset=offset)
        if not constructors:
            break
        all_constructors.extend(constructors)
        offset += 30  # Limite de construtores por página

# Cria o DataFrame com todos os dados de construtores
df = pd.DataFrame(all_constructors)
# Cria a engine para o banco de dados SQLite
engine = create_engine('sqlite:///../data/F1_DATA.db')

# Salva o DataFrame em uma tabela chamada "drivers" no banco de dados SQLite
df.to_sql('constructors', con=engine, if_exists='replace', index=False)

print("Dados salvos no banco de dados com sucesso.")




# %%
# DADOS DOS CIRCUITOS USANDO A API ERGAST (NÃO SERÁ MAIS ATUALIZADA AO FINAL DE 2024) 

import requests
import pandas as pd
from tqdm import tqdm

def fetch_circuits(offset=0):
    url = f"http://ergast.com/api/f1/circuits.json?offset={offset}"
    response = requests.get(url)
    
    if response.status_code != 200:
        print("Erro ao obter os dados:", response.status_code)
        return []
    
    data = response.json()
    circuits = []
    for circuit in data['MRData']['CircuitTable']['Circuits']:
        circuit_data = {
            "circuitId": circuit['circuitId'],
            "url": circuit['url'],
            "circuitName": circuit['circuitName'],
            "locality": circuit['Location']['locality'],
            "country": circuit['Location']['country'],
            "latitude": circuit['Location']['lat'],
            "longitude": circuit['Location']['long'],
        }
        circuits.append(circuit_data)
    
    return circuits

# Inicializa a lista de todos os circuitos
all_circuits = []

# Limite de circuitos por página (30 por padrão, pode ser ajustado pela API)
limit_per_page = 30
offset = 0

# Usa tqdm para criar a barra de progresso
while True:
    circuits = fetch_circuits(offset=offset)
    if not circuits:
        break
    all_circuits.extend(circuits)
    offset += limit_per_page

# Cria o DataFrame com todos os dados de circuitos
df = pd.DataFrame(all_circuits)


engine = create_engine('sqlite:///../data/F1_DATA.db')
# Salva o DataFrame em uma tabela chamada "drivers" no banco de dados SQLite
df.to_sql('circuits', con=engine, if_exists='replace', index=False)
print("Dados salvos no banco de dados com sucesso.")


# %%
# DADOS DE CADA CORRIDA
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from tqdm import tqdm

def fetch_results(year, num_rounds):
    all_results = []

    for round_num in tqdm(range(1, num_rounds+1), desc=f"Progresso - {year}"):
        url = f"http://ergast.com/api/f1/{year}/{round_num}/results"
        response = requests.get(url)

        if response.status_code != 200:
            print(f"Erro ao obter os dados para {year}/{round_num}: {response.status_code}")
            continue

        root = ET.fromstring(response.text)
        race_table = root.find("{http://ergast.com/mrd/1.4}RaceTable")
        if race_table is None:
            print(f"Sem dados para {year}/{round_num}")
            continue  # Não há dados para essa corrida

        for result in race_table.findall(".//{http://ergast.com/mrd/1.4}Result"):
            result_data = {
                "year": year,
                "round": round_num,
                "number": result.get("number"),
                "position": result.get("position"),
                "points": result.get("points"),
                "grid": result.find("{http://ergast.com/mrd/1.4}Grid").text,
                "laps": result.find("{http://ergast.com/mrd/1.4}Laps").text,
                "status": result.find("{http://ergast.com/mrd/1.4}Status").text
            }
            driver = result.find("{http://ergast.com/mrd/1.4}Driver")
            result_data["driverId"] = driver.get("driverId")
            result_data["givenName"] = driver.find("{http://ergast.com/mrd/1.4}GivenName").text
            result_data["familyName"] = driver.find("{http://ergast.com/mrd/1.4}FamilyName").text
            result_data["dateOfBirth"] = driver.find("{http://ergast.com/mrd/1.4}DateOfBirth").text
            result_data["nationality"] = driver.find("{http://ergast.com/mrd/1.4}Nationality").text
            constructor = result.find("{http://ergast.com/mrd/1.4}Constructor")
            result_data["constructorId"] = constructor.get("constructorId")
            result_data["constructorName"] = constructor.find("{http://ergast.com/mrd/1.4}Name").text
            result_data["constructorNationality"] = constructor.find("{http://ergast.com/mrd/1.4}Nationality").text
            all_results.append(result_data)

    return all_results

# Anos para os quais você deseja buscar os resultados
anos = list(range(2023, 2024))
num_rounds = 22  # Assumindo que há 22 corridas em cada temporada

# Inicializa a lista de todos os resultados
all_results = []

# Itera sobre os anos e busca os resultados para cada ano
for year in anos:
    results = fetch_results(year, num_rounds)
    all_results.extend(results)

# Cria o DataFrame com todos os dados de resultados
df = pd.DataFrame(all_results)
print(df)


# %%

import requests

url = "http://ergast.com/api/f1/2023/20/results"

payload={}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)



