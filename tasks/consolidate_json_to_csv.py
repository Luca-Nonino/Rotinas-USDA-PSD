import pandas as pd
import json
import os

def convert_json_to_csv(data_dir, output_csv):
    # Definir o caminho para os diretórios
    world_dir = os.path.join(data_dir, 'raw', 'world')
    country_dir = os.path.join(data_dir, 'raw', 'country')

    # Lista para armazenar todas as linhas de dados
    data_rows = []

    # Processar cada diretório
    for directory in [world_dir, country_dir]:
        # Iterar através de cada arquivo no diretório
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            # Verificar se o arquivo é um arquivo JSON
            if file_path.endswith('.json'):
                with open(file_path, 'r') as file:
                    json_data = json.load(file)
                    # Extrair dados de cada entrada no arquivo JSON
                    for entry in json_data:
                        commodity_code = str(entry.get('CommodityCode', '')).lstrip('0') or '0'
                        row = {
                            'CommodityCode': int(commodity_code),
                            'CountryName': entry.get('CountryName', '').strip(),
                            'MarketYear': entry.get('MarketYear', ''),
                            'AttributeDescription': entry.get('AttributeDescription', '').replace(" ", ""),
                            'Value': entry.get('Value', ''),
                            'CalendarYear': entry.get('CalendarYear', ''),
                            'Month': entry.get('Month', '')
                        }
                        data_rows.append(row)

    # Criar um DataFrame
    df = pd.DataFrame(data_rows)

    # Escrever o DataFrame para CSV
    df.to_csv(output_csv, index=False)
    print(f"Os dados para foram consolidados em {output_csv}")

# Definir o diretório base de dados e o caminho do arquivo CSV de saída
base_dir = 'data'
output_csv_path = os.path.join(base_dir, 'processed', 'consolidated', 'consolidated_data.csv')

# Chamar a função para converter os dados JSON em CSV
convert_json_to_csv(base_dir, output_csv_path)