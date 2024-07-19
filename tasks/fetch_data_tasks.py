import os
import json
import datetime
import asyncio
import aiohttp
import backoff



# Definições gerais
api_key = '697486e5-932d-46d3-804a-388452a19d70'
country_data_dir = 'data/raw/country'
world_data_dir = 'data/raw/world'
commodity_codes = [
    '0440000', '2231000', '2223000', '2232000', '2221000', '2226000',
    '2222000', '2224000', '4232000', '0813100', '0410000', '4233000',
    '4235000', '4243000', '4244000', '4234000', '4239100', '4236000',
    '4242000', '0813700', '0813300', '0814200', '0813800', '0813200',
    '0813600', '0813500', '2631000'
]
log_file = 'data/logs/update_log.json'
error_log_file = 'data/logs/error_logs.txt'

# Função para ler o arquivo de log
def read_update_log():
    if os.path.exists(log_file):
        with open(log_file, 'r') as file:
            return json.load(file)
    return {"last_fetched": {"year": 0, "month": "0"}, "last_updated": {"year": 0, "month": "0"}}

# Função para atualizar o arquivo de log
def update_log_file(year, month):
    log_data = read_update_log()
    log_data["last_fetched"] = {"year": year, "month": month}
    with open(log_file, 'w') as file:
        json.dump(log_data, file)

# Função para registrar erros
def log_error(message):
    with open(error_log_file, 'a') as file:
        file.write(f"{datetime.datetime.now()}: {message}\n")

# Retry and error handling for fetching data from API
@backoff.on_exception(backoff.expo,
                      aiohttp.ClientError,
                      max_tries=5,
                      on_giveup=lambda x: log_error(f"Permanent failure for {x.args[0].url} after retries"))
async def fetch_data(session, url):
    try:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()
            elif response.status >= 500:
                message = f"Server error from {url}. Status code: {response.status}"
                log_error(message)
                response.raise_for_status()
            else:
                message = f"Request failed for {url}. Status code: {response.status}"
                log_error(message)
                return None
    except asyncio.TimeoutError:
        log_error(f"Timeout while fetching data from {url}")

# Função assíncrona para salvar dados em arquivo JSON
async def save_data(data, path):
    if data:
        try:
            with open(path, 'w') as file:
                json.dump(data, file)
        except IOError as e:
            log_error(f"Failed writing to {path}: {e}")

# Função assíncrona para buscar e salvar dados
async def fetch_and_save_data(session, year, code, data_dir, api_endpoint, data_type):
    file_path = os.path.join(data_dir, f"{code}_{data_type}_data_{year}.json")
    url = f"{api_endpoint}?commodityCode={code}&marketYear={year}"
    data = await fetch_data(session, url)
    if data:
        await save_data(data, file_path)
    else:
        log_error(f"No data fetched for {url}, skipping save.")

async def fetch_current_period_data(session):
    current_date = datetime.datetime.now()
    print(f"Current date: {current_date}")
    year_to_fetch = current_date.year if current_date.month >= 5 else current_date.year - 1
    print(f"Year to fetch: {year_to_fetch}")
    commodity_code = '0410000'  # Example commodity code
    api_endpoint = "https://apps.fas.usda.gov/PSDOnlineDataServices/api/CommodityData/GetCommodityDataByYear"

    # Attempt to fetch data for the computed year
    url = f"{api_endpoint}?commodityCode={commodity_code}&marketYear={year_to_fetch}"
    print(f"Fetching data from: {url}")
    data = await fetch_data(session, url)
    if not data:
        # Fall back to the previous year if no data was found
        url = f"{api_endpoint}?commodityCode={commodity_code}&marketYear={year_to_fetch - 1}"
        print(f"Fetching data from: {url}")
        data = await fetch_data(session, url)
        if not data:
            raise Exception("Failed to fetch data for current and previous years for period determination")

    # Assuming all entries in the fetched data share the same month and year
    first_entry = data[0]
    current_year = int(first_entry['CalendarYear'])
    current_month = int(first_entry['Month'])
    print(f"Current year: {current_year}, Current month: {current_month}")
    return current_year, current_month


# Função principal que organiza a execução do script
async def main():
    os.makedirs(country_data_dir, exist_ok=True)
    os.makedirs(world_data_dir, exist_ok=True)

    async with aiohttp.ClientSession() as session:
        headers = {'Accept': 'application/json', 'API_KEY': api_key}
        session.headers.update(headers)
        
        # Fetch current year and month based on latest data
        current_year, current_month = await fetch_current_period_data(session)

        log_data = read_update_log()
        start_year = (current_year - 10) if current_month >= 5 else (current_year - 11)
        
        if log_data['last_fetched']['year'] == current_year and log_data['last_fetched']['month'] == str(current_month).zfill(2):
            print("Os dados para o mês atual já estão atualizados.")
            return
        
        tasks = []
        for year in range(start_year, current_year + 1):
            for code in commodity_codes:
                country_url = "https://apps.fas.usda.gov/PSDOnlineDataServices/api/CommodityData/GetCommodityDataByYear"
                world_url = "https://apps.fas.usda.gov/PSDOnlineDataServices/api/CommodityData/GetWorldCommodityDataByYear"
                tasks.append(fetch_and_save_data(session, year, code, country_data_dir, country_url, "country"))
                tasks.append(fetch_and_save_data(session, year, code, world_data_dir, world_url, "world"))

        await asyncio.gather(*tasks)

    update_log_file(current_year, str(current_month).zfill(2))
    print("Busca e salvamento de dados completos.")

if __name__ == '__main__':
    asyncio.run(main())