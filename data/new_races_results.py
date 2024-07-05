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

def extract_race_data(year, round_number):
    url = f"http://ergast.com/api/f1/{year}/{round_number}/results"
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

def get_most_recent_race(engine):
    query = """
    SELECT DISTINCT Season, Round
    FROM results
    ORDER BY Season DESC, Round DESC
    LIMIT 1
    """
    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchone()
    if result:
        return result[0], result[1]  # Acessar os resultados como uma tupla
    return None, None

def save_new_race_data(df, engine):
    df.to_sql('results', con=engine, if_exists='append', index=False)
    logger.info("Dados novos salvos no banco de dados com sucesso.")

engine = create_engine('sqlite:///../data/F1_DATA.db')

most_recent_season, most_recent_round = get_most_recent_race(engine)

if most_recent_season is None:
    logger.info("Nenhum dado encontrado no banco de dados, iniciando do ano 2000.")
    years = range(2000, 2025)
else:
    logger.info(f"Dados encontrados até a temporada {most_recent_season}, rodada {most_recent_round}.")
    years = range(most_recent_season, 2025)

all_race_data = []
new_races = False

for year in tqdm(years, desc="Fetching race data by year"):
    total_rounds = get_total_rounds(year)
    start_round = 1
    if year == most_recent_season:
        start_round = most_recent_round + 1

    for round_number in tqdm(range(start_round, total_rounds + 1), desc=f"Year {year}", leave=False):
        logger.info(f"Buscando dados para ano {year}, rodada {round_number}.")
        race_data = extract_race_data(year, round_number)
        if race_data:
            all_race_data.extend(race_data)
            new_races = True

if new_races:
    df = pd.DataFrame(all_race_data)
    df['Season'] = df['Season'].astype(int)
    df['Round'] = df['Round'].astype(int)
    df['Position'] = df['Position'].astype(int)
    df['Points'] = df['Points'].astype(float)
    df['PermanentNumber'] = df['PermanentNumber'].astype(int)
    df['Grid'] = df['Grid'].astype(int)
    df['Laps'] = df['Laps'].astype(int)
    
    save_new_race_data(df, engine)
else:
    logger.info("Não há novas corridas para serem adicionadas.")

