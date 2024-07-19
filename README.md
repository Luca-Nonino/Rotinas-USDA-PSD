

# Documentação do Projeto de Pipeline de Dados do USDA

## Descrição Geral

Este projeto é uma pipeline de dados desenvolvida para coletar, processar e gerar arquivos de dados históricos internacionais e nacionais do Departamento de Agricultura dos Estados Unidos (USDA). O sistema é usado internamente e não está aberto a contribuições externas. O foco está em processar e disponibilizar informações agrícolas para uso analítico dentro da empresa.

## Estrutura do Projeto

O projeto está organizado da seguinte forma:

- **data/usda**: Diretório que contém todos os arquivos brutos, processados e logs de erro.
  - **ipv_files**: Guarda os arquivos IPV gerados mensalmente.
  - **logs**: Contém os logs de erros e atualizações.
  - **processed**: Arquivos como listas IPV e referências consolidadas.
  - **raw**: Dados brutos coletados de APIs, organizados por país e mundo.
- **tasks**: Scripts Python específicos para cada tarefa de processamento de dados.
- **main.py**: Ponto de entrada do script que coordena as tarefas de coleta e processamento de dados.

## Configuração e Instalação

Certifique-se de que Python 3.8+ está instalado no seu ambiente. Clone o repositório para o ambiente local onde a pipeline será executada. As dependências necessárias estão listadas em `requirements.txt`, instale-as usando:

```bash
pip install -r requirements.txt
```

## Executando o Projeto

Para iniciar o processo de coleta e processamento de dados, execute o script `main.py` a partir da linha de comando:

```bash
python main.py
```

Este script realiza as seguintes operações em sequência:
1. Coleta dados dos últimos 10 anos através de APIs do USDA, considerando tanto dados nacionais quanto internacionais.
2. Processa os dados JSON brutos em formatos CSV consolidados.
3. Gera arquivos IPV baseados nos dados processados para análises específicas.

O output principal são os arquivos IPV gerados mensalmente, organizados por código de commodity e país, localizados em `data/usda/ipv_files/{ano}_{mês}`.

## Logs

- **Logs de erro**: Registrados em `data/usda/logs/error_logs.txt`, contêm detalhes de falhas durante as operações.
- **Logs de atualização**: Registrados em `data/usda/logs/update_log.json`, rastreiam os últimos dados coletados com sucesso.

## Limpeza de Dados

O script `main.py` inclui uma rotina para limpar diretórios de dados brutos e processados após cada ciclo completo de execução para evitar a acumulação de arquivos desnecessários e manter a organização do sistema de arquivos.

---

Certifique-se de manter esta documentação atualizada conforme mudanças e melhorias são implementadas no projeto. Esta documentação serve como um guia inicial para novos desenvolvedores e para a equipe de manutenção entender e operar a pipeline de dados com eficácia.