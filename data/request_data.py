# %%

import requests
import xml.etree.ElementTree as ET
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
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
df = df.sort_values(by=['DateOfBirth'], ascending=False, ignore_index=True)

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

df['year'] = df['year'].astype(int)
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
# DADOS DOS CORRIDAS USANDO A API ERGAST (NÃO SERÁ MAIS ATUALIZADA AO FINAL DE 2024) 

import requests
import xml.etree.ElementTree as ET
import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine

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
        fastest_lap_lap = result.find('ns:FastestLap', namespace).attrib.get('lap') if result.find('ns:FastestLap', namespace) is not None else 0
        
        average_speed_element = result.find('ns:FastestLap/ns:AverageSpeed', namespace)
        fastest_lap_speed = average_speed_element.text if average_speed_element is not None else 0
        
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
            'FastestLapTime': fastest_lap_time,
            'FastestLapLap': fastest_lap_lap,
            'AverageSpeed': fastest_lap_speed
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

years = [2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,
         2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,
         2021,2022,2023,2024]
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
df['Season'] =df['Season'].astype(int)
df['Round']  =df['Round'] .astype(int)
df['Position'] = df['Position'] .astype(int)
df['Points'] = df['Points'] .astype(float)
df['PermanentNumber'] =df['PermanentNumber'].astype(int)
df['Grid'] =df['Grid'].astype(int)
df['Laps'] =df['Laps'].astype(int)



engine = create_engine('sqlite:///../data/F1_DATA.db')
# Salva o DataFrame em uma tabela chamada "results" no banco de dados SQLite
df.to_sql('results', con=engine, if_exists='replace', index=False)
print("Dados salvos no banco de dados com sucesso.")


# %%

# DADOS DOS QUALIFICAÇÃO USANDO A API ERGAST (NÃO SERÁ MAIS ATUALIZADA AO FINAL DE 2024) 

import requests
import xml.etree.ElementTree as ET
import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine

def extract_qualifying_data(year, round_number):
    url = f"http://ergast.com/api/f1/{year}/{round_number}/qualifying"
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

    for qualifying_result in root.findall('.//ns:QualifyingResult', namespace):
        position = qualifying_result.attrib.get('position')
        driver = qualifying_result.find('ns:Driver', namespace)
        driver_id = driver.attrib.get('driverId')
        code = driver.attrib.get('code')
        permanent_number = driver.find('ns:PermanentNumber', namespace).text if driver.find('ns:PermanentNumber', namespace) is not None else 0
        given_name = driver.find('ns:GivenName', namespace).text if driver.find('ns:GivenName', namespace) is not None else 0
        family_name = driver.find('ns:FamilyName', namespace).text if driver.find('ns:FamilyName', namespace) is not None else 0
        date_of_birth = driver.find('ns:DateOfBirth', namespace).text if driver.find('ns:DateOfBirth', namespace) is not None else 0
        nationality = driver.find('ns:Nationality', namespace).text if driver.find('ns:Nationality', namespace) is not None else 0
        
        constructor = qualifying_result.find('ns:Constructor', namespace)
        constructor_id = constructor.attrib.get('constructorId')
        constructor_name = constructor.find('ns:Name', namespace).text if constructor.find('ns:Name', namespace) is not None else 0
        constructor_nationality = constructor.find('ns:Nationality', namespace).text if constructor.find('ns:Nationality', namespace) is not None else 0
        
        q1 = qualifying_result.find('ns:Q1', namespace).text if qualifying_result.find('ns:Q1', namespace) is not None else 0
        q2 = qualifying_result.find('ns:Q2', namespace).text if qualifying_result.find('ns:Q2', namespace) is not None else 0
        q3 = qualifying_result.find('ns:Q3', namespace).text if qualifying_result.find('ns:Q3', namespace) is not None else 0
        
        data.append({
            'Season': season,
            'Round': round_number,
            'CircuitID': circuit_id,
            'Position': position,
            'DriverID': driver_id,
            'Code': code,
            'PermanentNumber': permanent_number,
            'GivenName': given_name,
            'FamilyName': family_name,
            'DateOfBirth': date_of_birth,
            'Nationality': nationality,
            'ConstructorID': constructor_id,
            'ConstructorName': constructor_name,
            'ConstructorNationality': constructor_nationality,
            'Q1': q1,
            'Q2': q2,
            'Q3': q3
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

years = list(range(2000, 2025))
all_qualifying_data = []

# Usar tqdm para exibir uma barra de progresso
for year in tqdm(years, desc="Fetching qualifying data by year"):
    total_rounds = get_total_rounds(year)
    for round_number in tqdm(range(1, total_rounds + 1), desc=f"Year {year}", leave=False):
        qualifying_data = extract_qualifying_data(year, round_number)
        all_qualifying_data.extend(qualifying_data)

# Criar o dataframe com todos os dados
df = pd.DataFrame(all_qualifying_data)

# Mostrar o dataframe
df

df['Season'] =df['Season'].astype(int)
df['Round']  =df['Round'] .astype(int)
df['Position'] = df['Position'] .astype(int)
df['PermanentNumber'] =df['PermanentNumber'].astype(int)


engine = create_engine('sqlite:///../data/F1_DATA.db')
# Salva o DataFrame em uma tabela chamada "qualifying_results" no banco de dados SQLite
df.to_sql('qualifying_results', con=engine, if_exists='replace', index=False)
print("Dados salvos no banco de dados com sucesso.")

# %%

# DRIVERS STANDINGS
import requests
import pandas as pd
from tqdm import tqdm

# Função para extrair informações de uma corrida específica
def extract_driver_standings(season, round_num):
    url = f"http://ergast.com/api/f1/{season}/{round_num}/driverStandings"
    response = requests.get(url)
    
    if response.status_code != 200:
        return None

    # Parse do XML
    data = response.text
    if "<StandingsTable" not in data:
        return None
    
    root = ET.fromstring(data)
    namespace = {'ns': 'http://ergast.com/mrd/1.5'}
    
    standings_table = root.find('ns:StandingsTable', namespace)
    if standings_table is None:
        return None
    
    standings_list = standings_table.find('ns:StandingsList', namespace)
    if standings_list is None:
        return None

    season = standings_table.get('season')
    round_num = standings_list.get('round')
    
    standings = []
    
    for driver_standing in standings_list.findall('ns:DriverStanding', namespace):
        position = driver_standing.get('position')
        points = driver_standing.get('points')
        wins = driver_standing.get('wins')
        driver = driver_standing.find('ns:Driver', namespace)
        driver_id = driver.get('driverId')
        
        standings.append({
            'season': season,
            'round': round_num,
            'position': position,
            'points': points,
            'wins': wins,
            'driverId': driver_id
        })
    
    return standings

# Dataframe para armazenar os dados
all_standings = pd.DataFrame(columns=['season', 'round', 'position', 'points', 'wins', 'driverId'])

# Loop pelos anos e rodadas
for year in tqdm(range(2000, 2025)):
    round_num = 1
    while True:
        standings = extract_driver_standings(year, round_num)
        if standings is None:
            break
        all_standings = pd.concat([all_standings, pd.DataFrame(standings)], ignore_index=True)
        round_num += 1

# Salvar os dados em um arquivo CSV
#all_standings.to_csv('driver_standings.csv', index=False)

# Exibir os primeiros registros
all_standings['season'] = all_standings['season'].astype(int)
all_standings['round'] = all_standings['round'].astype(int)
all_standings['position'] = all_standings['position'].astype(int)
all_standings['points'] = all_standings['points'].astype(float)
all_standings['wins'] = all_standings['wins'].astype(int)

engine = create_engine('sqlite:///../data/F1_DATA.db')
# Salva o DataFrame em uma tabela chamada "qualifying_results" no banco de dados SQLite
all_standings.to_sql('driver_standings', con=engine, if_exists='replace', index=False)
print("Dados salvos no banco de dados com sucesso.")

# %%
import requests
import pandas as pd
from tqdm import tqdm
import xml.etree.ElementTree as ET

# Função para extrair informações de uma corrida específica
def extract_constructor_standings(season, round_num):
    url = f"http://ergast.com/api/f1/{season}/{round_num}/constructorStandings"
    response = requests.get(url)
    
    if response.status_code != 200:
        return None

    # Parse do XML
    data = response.text
    root = ET.fromstring(data)
    namespace = {'ns': 'http://ergast.com/mrd/1.5'}
    
    standings_table = root.find('ns:StandingsTable', namespace)
    if standings_table is None:
        return None
    
    standings_list = standings_table.find('ns:StandingsList', namespace)
    if standings_list is None:
        return None

    season = standings_table.get('season')
    round_num = standings_list.get('round')
    
    standings = []
    
    for constructor_standing in standings_list.findall('ns:ConstructorStanding', namespace):
        position = constructor_standing.get('position')
        points = constructor_standing.get('points')
        wins = constructor_standing.get('wins')
        constructor = constructor_standing.find('ns:Constructor', namespace)
        constructor_id = constructor.get('constructorId')
        
        standings.append({
            'season': season,
            'round': round_num,
            'position': position,
            'points': points,
            'wins': wins,
            'constructorId': constructor_id
        })
    
    return standings

# Dataframe para armazenar os dados
all_standings = pd.DataFrame(columns=['season', 'round', 'position', 'points', 'wins', 'constructorId'])

# Loop pelos anos e rodadas
for year in tqdm(range(2000, 2025)):
    round_num = 1
    while True:
        standings = extract_constructor_standings(year, round_num)
        if standings is None:
            break
        all_standings = pd.concat([all_standings, pd.DataFrame(standings)], ignore_index=True)
        round_num += 1

# Exibir os primeiros registros
all_standings
all_standings['season'] = all_standings['season'].astype(int)
all_standings['round'] = all_standings['round'].astype(int)
all_standings['position'] = all_standings['position'].astype(int)
all_standings['points'] = all_standings['points'].astype(float)
all_standings['wins'] = all_standings['wins'].astype(int)

engine = create_engine('sqlite:///../data/F1_DATA.db')
# Salva o DataFrame em uma tabela chamada "qualifying_results" no banco de dados SQLite
all_standings.to_sql('constructor_standings', con=engine, if_exists='replace', index=False)
print("Dados salvos no banco de dados com sucesso.")

# %%

import requests
import pandas as pd
from tqdm import tqdm
import xml.etree.ElementTree as ET

# Função para extrair informações de uma corrida específica
def extract_pitstops(season, round_num):
    url = f"http://ergast.com/api/f1/{season}/{round_num}/pitstops"
    response = requests.get(url)
    
    if response.status_code != 200:
        return None

    # Parse do XML
    data = response.text
    root = ET.fromstring(data)
    namespace = {'ns': 'http://ergast.com/mrd/1.5'}
    
    race_table = root.find('ns:RaceTable', namespace)
    if race_table is None:
        return None
    
    race = race_table.find('ns:Race', namespace)
    if race is None:
        return None

    season = race_table.get('season')
    round_num = race.get('round')
    circuit_id = race.find('ns:Circuit', namespace).get('circuitId')
    
    pitstops = []
    
    for pitstop in race.findall('ns:PitStopsList/ns:PitStop', namespace):
        driver_id = pitstop.get('driverId')
        stop = pitstop.get('stop')
        lap = pitstop.get('lap')
        duration = pitstop.get('duration')
        
        pitstops.append({
            'season': season,
            'round': round_num,
            'circuitId': circuit_id,
            'driverId': driver_id,
            'stop': stop,
            'lap': lap,
            'duration': duration
        })
    
    return pitstops

# Dataframe para armazenar os dados
all_pitstops = pd.DataFrame(columns=['season', 'round', 'circuitId', 'driverId', 'stop', 'lap', 'duration'])

# Loop pelos anos e rodadas
for year in tqdm(range(2000, 2025)):
    round_num = 1
    while True:
        pitstops = extract_pitstops(year, round_num)
        if pitstops is None:
            break
        all_pitstops = pd.concat([all_pitstops, pd.DataFrame(pitstops)], ignore_index=True)
        round_num += 1

# Exibir os primeiros registros
all_pitstops
all_pitstops['season'] = all_pitstops['season'].astype(int)
all_pitstops['round'] = all_pitstops['round'].astype(int)
all_pitstops['stop'] = all_pitstops['stop'].astype(int)
all_pitstops['lap'] = all_pitstops['lap'].astype(int)

engine = create_engine('sqlite:///../data/F1_DATA.db')
# Salva o DataFrame em uma tabela chamada "qualifying_results" no banco de dados SQLite
all_pitstops.to_sql('pitstops', con=engine, if_exists='replace', index=False)
print("Dados salvos no banco de dados com sucesso.")
