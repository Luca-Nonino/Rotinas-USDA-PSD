import sys
import os
import logging
import asyncio
from io import StringIO
import time
import shutil
import six
from tasks.fetch_data_tasks import main as fetch_data
from tasks.generate_ipv_files_tasks import generate_historical_ipvs
from tasks.consolidate_ipvs import consolidate_ipv_files
from tasks.consolidate_json_to_csv import convert_json_to_csv

# Configura logging
logging.basicConfig(filename='data/logs/error_logs.txt', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

async def capture_fetch_data():
    # Cria um pipe para capturar stdout da coroutine
    old_stdout = sys.stdout
    sys.stdout = StringIO()
    await fetch_data()  # Usa await para chamar corretamente a função assíncrona
    output = sys.stdout.getvalue().strip()
    sys.stdout = old_stdout  # Restaura stdout
    return output

async def process_data():
    # Define os caminhos base e de saída para processamento de dados
    base_dir = 'data'
    output_csv_path = os.path.join(base_dir, 'processed', 'consolidated', 'consolidated_data.csv')
    convert_json_to_csv(base_dir, output_csv_path)  # Converte JSON para CSV
    await generate_historical_ipvs()  # Gera arquivos históricos IPV
    consolidate_ipv_files()  # Consolida arquivos IPV


def clear_directories():
    # Lista os diretórios para limpeza e realiza a exclusão de arquivos e diretórios internos
    directories = [
        'data/raw/country',
        'data/raw/world',
        'data/processed/consolidated'
    ]
    for directory in directories:
        for item in os.listdir(directory):
            item_path = os.path.join(directory, item)
            if os.path.isfile(item_path):
                os.remove(item_path)
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)

async def main():
    # Inicia o cronômetro para medir o tempo de execução do script
    start_time = time.time()
    try:
        fetch_data_output = await capture_fetch_data()

        logging.info(fetch_data_output)  # Loga o output da captura de dados

        if "Os dados para o mês atual já estão atualizados." in fetch_data_output:
            logging.info("Dados para o mês atual já estão atualizados.")
            print("Dados para o mês atual já estão atualizados.")
        else:
            await process_data()

    except Exception as e:
        logging.exception("Ocorreu um erro durante o processamento de dados.")
        print(f"Ocorreu um erro durante o processamento de dados: {str(e)}")
    finally:
        clear_directories()  # Limpa os diretórios especificados
        execution_time = time.time() - start_time
        print(f"Tempo de execução: {execution_time:.2f} segundos")

if __name__ == "__main__":
    asyncio.run(main())  # Usa asyncio.run para lidar com a função assíncrona principal