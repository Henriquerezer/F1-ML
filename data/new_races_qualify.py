# %%
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine, text
import logging

# Configurar o logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_qualifying_data(year, round_number):
    url = f"http://ergast.com/api/f1/{year}/{round_number}/qualifying"
    response = requests.get(url)
    response_text = response.text
    root = ET.fromstring(response_text)

    namespace = {'ns': 'http://ergast.com/mrd/1.5'}
    data = []

    race_table = root.find('.//ns:RaceTable', namespace)
    if race_table is None:
        logger.warning(f"No RaceTable found for year {year}, round {round_number}.")
        return data
    
    season = race_table.attrib.get('season')
    round_number = race_table.attrib.get('round')
    race = race_table.find('.//ns:Race', namespace)
    if race is None:
        logger.warning(f"No Race found for year {year}, round {round_number}.")
        return data
    
    circuit = race.find('ns:Circuit', namespace)
    if circuit is None:
        logger.warning(f"No Circuit found for year {year}, round {round_number}.")
        return data
    
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

def get_most_recent_qualifying(engine):
    query = """
    SELECT DISTINCT Season, Round
    FROM qualifying_results
    ORDER BY Season DESC, Round DESC

    LIMIT 1
    """
    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchone()
    if result:
        return result[0], result[1]  # Acessar os resultados como uma tupla
    return None, None

def save_new_qualifying_data(df, engine):
    df.to_sql('qualifying_results', con=engine, if_exists='append', index=False)
    logger.info("Dados novos salvos no banco de dados com sucesso.")

engine = create_engine('sqlite:///../data/F1_DATA.db')

most_recent_season, most_recent_round = get_most_recent_qualifying(engine)

if most_recent_season is None:
    logger.info("Nenhum dado encontrado no banco de dados, iniciando do ano 2000.")
    years = range(2000, 2025)
else:
    logger.info(f"Dados encontrados até a temporada {most_recent_season}, rodada {most_recent_round}.")
    years = range(most_recent_season, 2025)

all_qualifying_data = []
new_qualifying = False

for year in tqdm(years, desc="Fetching qualifying data by year"):
    total_rounds = get_total_rounds(year)
    start_round = 1
    if year == most_recent_season:
        start_round = most_recent_round + 1

    for round_number in tqdm(range(start_round, total_rounds + 1), desc=f"Year {year}", leave=False):
        logger.info(f"Buscando dados de qualificação para ano {year}, rodada {round_number}.")
        qualifying_data = extract_qualifying_data(year, round_number)
        if qualifying_data:
            all_qualifying_data.extend(qualifying_data)
            new_qualifying = True

if new_qualifying:
    df = pd.DataFrame(all_qualifying_data)
    df['Season'] = df['Season'].astype(int)
    df['Round'] = df['Round'].astype(int)
    df['Position'] = df['Position'].astype(int)
    df['PermanentNumber'] = df['PermanentNumber'].astype(int)
    
    save_new_qualifying_data(df, engine)
else:
    logger.info("Não há novos dados de qualificação para serem adicionados.")

# %%
