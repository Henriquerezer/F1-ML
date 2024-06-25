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
from sqlalchemy import create_engine
import sqlalchemy

def extract_race_data(year, round_number):
    url = f"http://ergast.com/api/f1/{year}/{round_number}/results"
    response = requests.get(url)
    response_text = response.text
    root = ET.fromstring(response_text)

    namespace = {'ns': 'http://ergast.com/mrd/1.5'}
    data = []

    race_table = root.find('.//ns:RaceTable', namespace)
    season = race_table.attrib.get('season')
    round_number = race_table.attrib.get('round')
    race = race_table.find('.//ns:Race', namespace)
    circuit = race.find('ns:Circuit', namespace)
    circuit_id = circuit.attrib.get('circuitId')

    for result in root.findall('.//ns:Result', namespace):
        position = result.attrib.get('position')
        points = result.attrib.get('points')
        
        driver = result.find('ns:Driver', namespace)
        driver_id = driver.attrib.get('driverId')
        code = driver.attrib.get('code')
        permanent_number = driver.find('ns:PermanentNumber', namespace).text
        given_name = driver.find('ns:GivenName', namespace).text
        family_name = driver.find('ns:FamilyName', namespace).text
        date_of_birth = driver.find('ns:DateOfBirth', namespace).text
        nationality = driver.find('ns:Nationality', namespace).text
        
        constructor = result.find('ns:Constructor', namespace)
        constructor_name = constructor.find('ns:Name', namespace).text
        constructor_nationality = constructor.find('ns:Nationality', namespace).text
        
        grid = result.find('ns:Grid', namespace).text
        laps = result.find('ns:Laps', namespace).text
        status = result.find('ns:Status', namespace).text
        time_element = result.find('ns:Time', namespace)
        time = time_element.text if time_element is not None else 0
        fastest_lap = result.find('ns:FastestLap/ns:Time', namespace)
        fastest_lap_time = fastest_lap.text if fastest_lap is not None else 0
        
        data.append({
            'Season': season,
            'Round': round_number,
            'CircuitID': circuit_id,
            'Position': position,
            'Points': points,
            'DriverID': driver_id,
            'Code': code,
            'PermanentNumber': permanent_number,
            'GivenName': given_name,
            'FamilyName': family_name,
            'DateOfBirth': date_of_birth,
            'Nationality': nationality,
            'ConstructorName': constructor_name,
            'ConstructorNationality': constructor_nationality,
            'Grid': grid,
            'Laps': laps,
            'Status': status,
            'Time': time,
            'FastestLapTime': fastest_lap_time
        })
    
    return data

def get_total_rounds(year):
    url = f"http://ergast.com/api/f1/{year}.xml"
    response = requests.get(url)
    response_text = response.text
    root = ET.fromstring(response_text)

    namespace = {'ns': 'http://ergast.com/mrd/1.5'}
    total_rounds = len(root.findall('.//ns:Race', namespace))
    return total_rounds

years = [2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023,2024]
all_race_data = []

# Usar tqdm para exibir uma barra de progresso
for year in tqdm(years, desc="Fetching race data by year"):
    total_rounds = get_total_rounds(year)
    for round_number in tqdm(range(1, total_rounds + 1), desc=f"Year {year}", leave=False):
        race_data = extract_race_data(year, round_number)
        all_race_data.extend(race_data)

# Criar o dataframe com todos os dados
df = pd.DataFrame(all_race_data)

engine = create_engine('sqlite:///../data/F1_DATA.db')
# Salva o DataFrame em uma tabela chamada "drivers" no banco de dados SQLite
df.to_sql('results', con=engine, if_exists='replace', index=False)
print("Dados salvos no banco de dados com sucesso.")

# %%
# DADOS DE NOVAS CORRIDAS

import requests
import xml.etree.ElementTree as ET
import pandas as pd
from tqdm import tqdm

def extract_race_data(year, round_number):
    url = f"http://ergast.com/api/f1/{year}/{round_number}/results"
    response = requests.get(url)
    response_text = response.text
    root = ET.fromstring(response_text)

    namespace = {'ns': 'http://ergast.com/mrd/1.5'}
    data = []

    race_table = root.find('.//ns:RaceTable', namespace)
    if race_table is None:
        return data  # Se não houver RaceTable, retornar lista vazia
    
    season = race_table.attrib.get('season')
    round_number = race_table.attrib.get('round')
    race = race_table.find('.//ns:Race', namespace)
    if race is None:
        return data  # Se não houver Race, retornar lista vazia
    
    circuit = race.find('ns:Circuit', namespace)
    if circuit is None:
        return data  # Se não houver Circuit, retornar lista vazia
    
    circuit_id = circuit.attrib.get('circuitId')

    for result in root.findall('.//ns:Result', namespace):
        position = result.attrib.get('position')
        points = result.attrib.get('points')
        
        driver = result.find('ns:Driver', namespace)
        driver_id = driver.attrib.get('driverId')
        code = driver.attrib.get('code')
        permanent_number = driver.find('ns:PermanentNumber', namespace).text if driver.find('ns:PermanentNumber', namespace) is not None else 0
        given_name = driver.find('ns:GivenName', namespace).text if driver.find('ns:GivenName', namespace) is not None else 0
        family_name = driver.find('ns:FamilyName', namespace).text if driver.find('ns:FamilyName', namespace) is not None else 0
        date_of_birth = driver.find('ns:DateOfBirth', namespace).text if driver.find('ns:DateOfBirth', namespace) is not None else 0
        nationality = driver.find('ns:Nationality', namespace).text if driver.find('ns:Nationality', namespace) is not None else 0
        
        constructor = result.find('ns:Constructor', namespace)
        constructor_name = constructor.find('ns:Name', namespace).text if constructor.find('ns:Name', namespace) is not None else 0
        constructor_nationality = constructor.find('ns:Nationality', namespace).text if constructor.find('ns:Nationality', namespace) is not None else 0
        
        grid = result.find('ns:Grid', namespace).text if result.find('ns:Grid', namespace) is not None else 0
        laps = result.find('ns:Laps', namespace).text if result.find('ns:Laps', namespace) is not None else 0
        status = result.find('ns:Status', namespace).text if result.find('ns:Status', namespace) is not None else 0
        time_element = result.find('ns:Time', namespace)
        time = time_element.text if time_element is not None else 0
        fastest_lap = result.find('ns:FastestLap/ns:Time', namespace)
        fastest_lap_time = fastest_lap.text if fastest_lap is not None else 0
        
        data.append({
            'Season': season,
            'Round': round_number,
            'CircuitID': circuit_id,
            'Position': position,
            'Points': points,
            'DriverID': driver_id,
            'Code': code,
            'PermanentNumber': permanent_number,
            'GivenName': given_name,
            'FamilyName': family_name,
            'DateOfBirth': date_of_birth,
            'Nationality': nationality,
            'ConstructorName': constructor_name,
            'ConstructorNationality': constructor_nationality,
            'Grid': grid,
            'Laps': laps,
            'Status': status,
            'Time': time,
            'FastestLapTime': fastest_lap_time
        })
    
    return data

def get_total_rounds(year):
    url = f"http://ergast.com/api/f1/{year}.xml"
    response = requests.get(url)
    response_text = response.text
    root = ET.fromstring(response_text)

    namespace = {'ns': 'http://ergast.com/mrd/1.5'}
    total_rounds = len(root.findall('.//ns:Race', namespace))
    return total_rounds

years = [2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023,2024]
all_race_data = []

# Usar tqdm para exibir uma barra de progresso
for year in tqdm(years, desc="Fetching race data by year"):
    total_rounds = get_total_rounds(year)
    for round_number in tqdm(range(1, total_rounds + 1), desc=f"Year {year}", leave=False):
        race_data = extract_race_data(year, round_number)
        all_race_data.extend(race_data)

# Criar o dataframe com todos os dados
df = pd.DataFrame(all_race_data)

# Mostrar o dataframe
df

engine = create_engine('sqlite:///../data/F1_DATA.db')
# Salva o DataFrame em uma tabela chamada "drivers" no banco de dados SQLite
df.to_sql('circuits', con=engine, if_exists='replace', index=False)
print("Dados salvos no banco de dados com sucesso.")
