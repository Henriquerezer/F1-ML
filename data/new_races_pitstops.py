# %%
import requests
import pandas as pd
from tqdm import tqdm
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine, text
import logging

# Configurar o logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levellevel)-8se)s - %(message)s')
logger = logging.getLogger(__name__)

# Função para extrair informações de uma corrida específica
def extract_pitstops(season, round_num):
    url = f"http://ergast.com/api/f1/{season}/{round_num}/pitstops"
    response = requests.get(url)
    
    if response.status_code != 200:
        logger.warning(f"Failed to retrieve data for season {season}, round {round_num}. Status code: {response.status_code}")
        return None

    # Parse do XML
    data = response.text
    root = ET.fromstring(data)
    namespace = {'ns': 'http://ergast.com/mrd/1.5'}
    
    race_table = root.find('ns:RaceTable', namespace)
    if race_table is None:
        logger.warning(f"No RaceTable found for season {season}, round {round_num}.")
        return None
    
    race = race_table.find('ns:Race', namespace)
    if race is None:
        logger.warning(f"No Race found for season {season}, round {round_num}.")
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

def get_most_recent_pitstop(engine):
    query = """
    SELECT DISTINCT season, round
    FROM pitstops
    ORDER BY season DESC, round DESC
    LIMIT 1
    """
    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchone()
    if result:
        return result[0], result[1]  # Acessar os resultados como uma tupla
    return None, None

def save_new_pitstop_data(df, engine):
    df.to_sql('pitstops', con=engine, if_exists='append', index=False)
    logger.info("Dados novos salvos no banco de dados com sucesso.")

engine = create_engine('sqlite:///../data/F1_DATA.db')

most_recent_season, most_recent_round = get_most_recent_pitstop(engine)

if most_recent_season is None:
    logger.info("Nenhum dado encontrado no banco de dados, iniciando do ano 2000.")
    years = range(2000, 2025)
else:
    logger.info(f"Dados encontrados até a temporada {most_recent_season}, rodada {most_recent_round}.")
    years = range(most_recent_season, 2025)

all_pitstops = []
new_pitstops = False

for year in tqdm(years, desc="Fetching pitstop data by year"):
    round_num = 1
    if year == most_recent_season:
        round_num = most_recent_round + 1
    
    while True:
        logger.info(f"Buscando dados de pitstop para ano {year}, rodada {round_num}.")
        pitstops = extract_pitstops(year, round_num)
        if pitstops is None:
            break
        all_pitstops.extend(pitstops)
        new_pitstops = True
        round_num += 1

if new_pitstops:
    df = pd.DataFrame(all_pitstops)
    df['season'] = df['season'].astype(int)
    df['round'] = df['round'].astype(int)
    df['stop'] = df['stop'].astype(int)
    df['lap'] = df['lap'].astype(int)
    
    save_new_pitstop_data(df, engine)
else:
    logger.info("Não há novos dados de pitstop para serem adicionados.")
