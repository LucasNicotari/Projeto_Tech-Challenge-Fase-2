"""
Configurações centralizadas do projeto B3 Data Pipeline
"""
import os
from pathlib import Path
from datetime import datetime
from env_loader import get_aws_config_from_env

class Config:
    """Classe de configuração centralizada"""
    
    # Diretórios
    BASE_DIR = Path(__file__).parent
    DOWNLOAD_DIR = BASE_DIR / "downloads"
    DATA_DIR = BASE_DIR / "data"
    LOGS_DIR = BASE_DIR / "logs"
    
    # URLs e seletores
    B3_URL = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"
    DOWNLOAD_XPATH = "//a[@href='/indexPage/day/IBOV?language=pt-br' and contains(text(), 'Download')]"
    
    # Configurações de arquivo
    CSV_ENCODING = 'latin1'
    CSV_SEPARATOR = ';'
    CSV_HEADER_ROW = 1
    
    # Padrões de nome de arquivo
    @staticmethod
    def get_csv_filename(date=None):
        if date is None:
            date = datetime.now().date()
        return f'IBOVDia_{date.strftime("%d-%m-%y")}.csv'
    
    @staticmethod
    def get_parquet_filename(date=None):
        csv_name = Config.get_csv_filename(date)
        return csv_name.replace('.csv', '.parquet')
    
    # AWS Configurações (usar variáveis de ambiente)
    @staticmethod
    def get_aws_config():
        """Obtém configurações AWS das variáveis de ambiente de forma segura"""
        return get_aws_config_from_env()
    
    # S3 Configurações
    @staticmethod
    def get_s3_key(date=None):
        if date is None:
            date = datetime.now().date()
        parquet_name = Config.get_parquet_filename(date)
        return f'raw/parquet file/{parquet_name}'
    
    # Timeouts e configurações de retry
    SELENIUM_TIMEOUT = 20
    DOWNLOAD_WAIT_TIME = 10
    MAX_RETRIES = 3
    
    # Validação de dados
    REQUIRED_COLUMNS = ['codigo', 'acao', 'tipo', 'qtdeteorica', 'part', 'data_pregao', 'timestamp_extracao']
    CODIGO_PATTERN = r'^[A-Z0-9]+$'
    
    @classmethod
    def create_directories(cls):
        """Cria diretórios necessários se não existirem"""
        for directory in [cls.DOWNLOAD_DIR, cls.DATA_DIR, cls.LOGS_DIR]:
            directory.mkdir(parents=True, exist_ok=True)
