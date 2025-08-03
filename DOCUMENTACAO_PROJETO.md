# Documentação do Projeto: Pipeline de Dados da B3

## 1. Visão Geral

Este projeto consiste em um pipeline de dados automatizado (ETL - Extract, Transform, Load) projetado para extrair a composição diária do índice IBOVESPA do site da B3, processar esses dados e, finalmente, carregá-los em um bucket na Amazon S3.

O pipeline é modular, robusto e configurável, seguindo boas práticas de desenvolvimento de software, como separação de responsabilidades, configuração centralizada e logging estruturado.

## 2. Arquitetura e Módulos

O projeto é dividido nos seguintes módulos, cada um com uma responsabilidade clara:

### `main_pipeline.py` - O Orquestrador
- **Responsabilidade**: Ponto de entrada principal que coordena a execução do pipeline.
- **Funcionamento**:
    1.  Inicializa todos os componentes necessários (Scraper, Processador, Uploader).
    2.  Executa a sequência de etapas: **Download -> Processamento -> Upload**.
    3.  Gerencia argumentos de linha de comando, permitindo execuções flexíveis (ex: pular o download, especificar uma data, limpar arquivos antigos).

### `scraper_module.py` - O Extrator (Web Scraping)
- **Responsabilidade**: Baixar o arquivo CSV com os dados do IBOVESPA diretamente do site da B3.
- **Tecnologia**: Utiliza a biblioteca **Selenium** para automatizar um navegador Google Chrome.
- **Funcionamento**:
    1.  Abre a página do IBOVESPA no site da B3.
    2.  Localiza e clica no botão de "Download".
    3.  Aguarda a conclusão do download e salva o arquivo no diretório `downloads/`.

### `data_processor_module.py` - O Transformador de Dados
- **Responsabilidade**: Limpar, validar e transformar os dados brutos do CSV.
- **Tecnologia**: Utiliza a biblioteca **Pandas** para manipulação de dados.
- **Funcionamento**:
    1.  Lê o arquivo CSV, que possui um formato específico (separador ';', codificação 'latin1', cabeçalhos a serem ignorados).
    2.  Realiza uma limpeza profunda: renomeia colunas, remove caracteres inválidos, converte tipos de dados (de texto para número) e filtra linhas que não representam ativos válidos.
    3.  Valida se o DataFrame processado contém as colunas essenciais.
    4.  Salva o DataFrame limpo e estruturado no formato **Parquet** no diretório `data/`, que é otimizado para performance e armazenamento.

### `s3_uploader_module.py` - O Carregador (Loader)
- **Responsabilidade**: Enviar o arquivo Parquet processado para a Amazon S3.
- **Tecnologia**: Utiliza a biblioteca **Boto3** (AWS SDK para Python).
- **Funcionamento**:
    1.  Conecta-se à AWS usando as credenciais fornecidas.
    2.  Valida o acesso ao bucket S3 configurado.
    3.  Faz o upload do arquivo Parquet para uma pasta particionada por data (ex: `raw/date=2025-08-01/`).
    4.  Adiciona metadados úteis ao objeto no S3, como data de processamento e versão do pipeline.

### Módulos de Suporte

- **`config_module.py`**: Centraliza todas as configurações do projeto, como URLs, caminhos de diretórios, nomes de arquivos e padrões. Isso facilita a manutenção e evita que valores fixos fiquem espalhados pelo código.
- **`logger_module.py`**: Fornece um sistema de logging padronizado que registra eventos importantes tanto no console quanto em arquivos de log. Utiliza emojis para diferenciar visualmente mensagens de sucesso, erro e processo.
- **`env_loader.py`**: Gerencia o carregamento de variáveis de ambiente a partir de um arquivo `.env`. É crucial para a segurança, pois permite que dados sensíveis (como credenciais da AWS) sejam mantidos fora do código-fonte.

## 3. Fluxo de Execução

1.  O usuário executa o `main_pipeline.py` via linha de comando.
2.  O `env_loader.py` carrega as credenciais da AWS e outras configurações do arquivo `.env`.
3.  O `scraper_module.py` é acionado para baixar o arquivo CSV do dia.
4.  O `data_processor_module.py` pega o CSV, o limpa e o converte para Parquet.
5.  O `s3_uploader_module.py` envia o arquivo Parquet final para o bucket S3.
6.  O `logger_module.py` registra cada passo do processo.

## 4. Como Executar

1.  **Configurar o ambiente**: Crie um arquivo `.env` com as credenciais da AWS e o nome do bucket S3.
2.  **Instalar dependências**: `pip install -r requirements.txt`
3.  **Executar o pipeline**: `python main_pipeline.py`
