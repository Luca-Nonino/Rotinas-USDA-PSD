import pandas as pd
import os
from datetime import datetime
import json
import asyncio
import aiofiles



async def update_log():
    log_path = os.path.join('data', 'logs', 'update_log.json')
    if os.path.exists(log_path):
        async with aiofiles.open(log_path, 'r') as file:
            log_data = json.loads(await file.read())
        log_data['last_updated'] = log_data['last_fetched']
        async with aiofiles.open(log_path, 'w') as file:
            await file.write(json.dumps(log_data))

async def write_ipv_file(data_frame, file_path):
    # Convert DataFrame to CSV format in memory first to control newline character
    csv_data = data_frame.to_csv(index=False)
    async with aiofiles.open(file_path, mode='w', newline='') as file:
        await file.write(csv_data)  # Escreve os dados em formato CSV no arquivo de forma assíncrona

async def generate_ipv_files(final_merged, output_dir):
    tasks = []
    for code, group in final_merged.groupby('cod'):
        tasks.append(write_file(code, group, output_dir))  # Cria uma tarefa para cada grupo de dados
    await asyncio.gather(*tasks)
    await update_log()  # Updates the log file after successful execution
    print(f"Número de arquivos IPV gerados: {len(tasks)}")  # Exibe o número de arquivos IPV gerados

async def write_file(code, group, output_dir):
    ipv_data = group.pivot(index='MarketYear', columns='header', values='Value').reset_index()
    ipv_data['data'] = ipv_data['MarketYear'].apply(lambda x: f"{x}-01-01")
    ipv_data.drop('MarketYear', axis=1, inplace=True)
    ipv_data.insert(0, 'cod', code)
    ipv_data.rename(columns={'cod': '<cod>', 'data': '<data>'}, inplace=True)
    headers = ['<cod>', '<data>'] + [col for col in ipv_data.columns if col not in ['<cod>', '<data>']]
    if not set(headers).issubset(ipv_data.columns):
        raise Exception("Expected headers are missing in the dataframe.")
    ipv_data = ipv_data[headers]  # Reorganiza as colunas com base nos cabeçalhos esperados
    file_name = f"{code.replace(':', '_')}_{datetime.now().year}_{datetime.now().strftime('%m')}.ipv"
    file_path = os.path.join(output_dir, file_name)
    await write_ipv_file(ipv_data, file_path)  # Chama função para escrever os dados no arquivo .ipv
    # print(f"Arquivo IPV gerado em {file_path}")  # Informa a geração do arquivo IPV

async def generate_historical_ipvs():
    data_dir = 'data'
    consolidated_csv_path = os.path.join(data_dir, 'processed', 'consolidated', 'consolidated_data.csv')
    ipv_reference_path = os.path.join(data_dir, 'processed', 'ipv_reference.csv')
    ipv_list_path = os.path.join(data_dir, 'processed', 'ipv_list.csv')

    if not os.path.exists(consolidated_csv_path) or os.stat(consolidated_csv_path).st_size == 0:
        # print(f"Erro: O arquivo {consolidated_csv_path} não existe ou está vazio.")
        return  # Sai da função se os dados consolidados não estiverem disponíveis

    consolidated_df = pd.read_csv(consolidated_csv_path)
    ipv_reference_df = pd.read_csv(ipv_reference_path, sep=';')
    ipv_list_df = pd.read_csv(ipv_list_path, sep=';')

    ipv_reference_df.rename(columns={'Country': 'CountryName'}, inplace=True)  # Renomeia a coluna para combinar com outras tabelas
    ipv_combined_df = ipv_reference_df.merge(ipv_list_df, on=['cod', 'CommodityCode'], how='left')  # Combina os DataFrames de referência
    final_merged = consolidated_df.merge(ipv_combined_df, on=['CountryName', 'CommodityCode', 'AttributeDescription'], how='left')
    final_merged.dropna(subset=['cod', 'header'], inplace=True)  # Remove quaisquer linhas com códigos ou cabeçalhos faltantes

    current_year = datetime.now().year
    current_month = datetime.now().strftime('%m')
    output_dir = os.path.join(data_dir, 'ipvs', f'{current_year}_{current_month}')  # Gera diretório de saída com mês e ano
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)  # Cria diretório se não existir

    await generate_ipv_files(final_merged, output_dir)  # Gera arquivos IPV para todos os grupos de dados

if __name__ == "__main__":
    generate_historical_ipvs()  # Ponto de entrada para gerar os IPV históricos