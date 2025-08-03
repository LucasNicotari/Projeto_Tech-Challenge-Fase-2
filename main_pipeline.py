"""
Pipeline principal para extração, processamento e upload dos dados B3
"""
import sys
import argparse
from datetime import datetime, date
from pathlib import Path
from typing import Optional

from config_module import Config
from logger_module import get_logger
from scraper_module import B3Scraper
from data_processor_module import DataProcessor
from s3_uploader_module import S3Uploader
from env_loader import initialize_environment

class B3DataPipeline:
    """Pipeline completo para dados da B3"""
    
    def __init__(self, skip_download: bool = False, skip_upload: bool = False):
        self.logger = get_logger('B3DataPipeline')
        self.skip_download = skip_download
        self.skip_upload = skip_upload
        
        # Inicializa componentes
        self.scraper = B3Scraper()
        self.processor = DataProcessor()
        self.uploader = S3Uploader() if not skip_upload else None
        
        # Cria diretórios necessários
        Config.create_directories()
    
    def run_pipeline(self, target_date: Optional[date] = None) -> bool:
        """
        Executa o pipeline completo
        
        Args:
            target_date: Data alvo para processamento (opcional)
            
        Returns:
            bool: True se sucesso, False caso contrário
        """
        if target_date is None:
            target_date = datetime.now().date()
        
        self.logger.info("=" * 60)
        self.logger.info(f"INICIANDO PIPELINE B3 - DATA: {target_date}")
        self.logger.info("=" * 60)
        
        try:
            csv_path = self._download_data(target_date)
            parquet_path = self._process_data(csv_path, target_date)
            
            if not self.skip_upload:
                upload_success = self._upload_data(parquet_path, target_date)
                if not upload_success:
                    return False
            
            self.logger.success("PIPELINE CONCLUÍDO COM SUCESSO!")
            return True
            
        except Exception as e:
            self.logger.failure(f"PIPELINE FALHOU: {e}")
            return False
    
    def _download_data(self, target_date: date) -> Path:
        """
        Etapa de download dos dados
        
        Args:
            target_date: Data alvo
            
        Returns:
            Path: Caminho do arquivo CSV baixado
        """
        self.logger.info("ETAPA 1: DOWNLOAD DOS DADOS")
        
        csv_filename = Config.get_csv_filename(target_date)
        csv_path = Config.DOWNLOAD_DIR / csv_filename
        
        if csv_path.exists() and self.skip_download:
            self.logger.info(f"Usando arquivo existente: {csv_path}")
            return csv_path
        
        if csv_path.exists():
            self.logger.warning(f"Arquivo já existe, será sobrescrito: {csv_path}")
        
        downloaded_path = self.scraper.download_ibov_data(target_date)
        
        if downloaded_path.name != csv_filename:
            new_path = Config.DOWNLOAD_DIR / csv_filename
            downloaded_path.rename(new_path)
            self.logger.info(f"Arquivo renomeado para: {csv_filename}")
            return new_path
        
        return downloaded_path
    
    def _process_data(self, csv_path: Path, target_date: date) -> Path:
        """
        Etapa de processamento dos dados
        
        Args:
            csv_path: Caminho do arquivo CSV
            target_date: Data alvo
            
        Returns:
            Path: Caminho do arquivo Parquet processado
        """
        self.logger.info("ETAPA 2: PROCESSAMENTO DOS DADOS")
        
        df = self.processor.process_csv_to_dataframe(csv_path)
        
        summary = self.processor.get_data_summary(df)
        self.logger.info(f"Resumo dos dados:")
        self.logger.info(f"   Total de empresas: {summary['total_companies']}")
        self.logger.info(f"   Total de linhas: {summary['total_rows']}")
        self.logger.info(f"   Participação média: {summary['average_participation']:.4f}%")
        
        parquet_filename = Config.get_parquet_filename(target_date)
        parquet_path = Config.DATA_DIR / parquet_filename
        
        self.processor.save_to_parquet(df, parquet_path)
        
        return parquet_path
    
    def _upload_data(self, parquet_path: Path, target_date: date) -> bool:
        """
        Etapa de upload para S3
        
        Args:
            parquet_path: Caminho do arquivo Parquet
            target_date: Data alvo
            
        Returns:
            bool: True se sucesso, False caso contrário
        """
        self.logger.info("ETAPA 3: UPLOAD PARA S3")
        
        s3_key = Config.get_s3_key(target_date)
        
        metadata = {
            'pipeline_version': '1.0',
            'processing_date': datetime.now().isoformat(),
            'data_date': target_date.isoformat(),
            'source': 'b3-ibovespa'
        }
        
        success = self.uploader.upload_file(
            parquet_path, 
            s3_key=s3_key, 
            metadata=metadata
        )
        
        if success:
            object_info = self.uploader.get_object_info(s3_key)
            if object_info:
                self.logger.success(f"Arquivo verificado no S3:")
                self.logger.info(f"   Tamanho: {object_info['size']} bytes")
                self.logger.info(f"   Última modificação: {object_info['last_modified']}")
        
        return success
    
    def cleanup_old_files(self, days_to_keep: int = 7):
        """
        Remove arquivos antigos para economizar espaço
        
        Args:
            days_to_keep: Número de dias de arquivos para manter
        """
        self.logger.info(f"Limpando arquivos com mais de {days_to_keep} dias...")
        
        current_time = datetime.now().timestamp()
        cutoff_time = current_time - (days_to_keep * 24 * 3600)
        
        for directory in [Config.DOWNLOAD_DIR, Config.DATA_DIR]:
            if not directory.exists():
                continue
                
            removed_count = 0
            for file_path in directory.glob('*'):
                if file_path.is_file() and file_path.stat().st_mtime < cutoff_time:
                    try:
                        file_path.unlink()
                        removed_count += 1
                        self.logger.debug(f"Removido: {file_path.name}")
                    except Exception as e:
                        self.logger.warning(f"Erro ao remover {file_path.name}: {e}")
            
            if removed_count > 0:
                self.logger.info(f"Removidos {removed_count} arquivos de {directory.name}")

def create_env_template():
    """Cria template de arquivo .env com as variáveis necessárias"""
    env_template = """# Configurações AWS - NUNCA commitar essas informações!
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_SESSION_TOKEN=your_session_token_here  # Opcional, para credenciais temporárias
AWS_REGION=us-east-1
S3_BUCKET_NAME=your-bucket-name

# Configurações opcionais
LOG_LEVEL=INFO
CLEANUP_DAYS=7
"""
    
    env_file = Path('.env.template')
    if not env_file.exists():
        env_file.write_text(env_template)
        print(f"Template de configuração criado: {env_file}")
        print("Configure suas credenciais AWS antes de executar o pipeline!")

def main():
    """Função principal com argumentos de linha de comando"""
    parser = argparse.ArgumentParser(description='Pipeline de dados B3 IBOVESPA')
    parser.add_argument('--skip-download', action='store_true', 
                       help='Pula o download se arquivo já existir')
    parser.add_argument('--skip-upload', action='store_true',
                       help='Pula o upload para S3')
    parser.add_argument('--date', type=str, 
                       help='Data específica no formato YYYY-MM-DD')
    parser.add_argument('--cleanup', action='store_true',
                       help='Remove arquivos antigos após execução')
    parser.add_argument('--create-env', action='store_true',
                       help='Cria template de arquivo .env')
    
    args = parser.parse_args()
    
    # Cria template .env se solicitado
    if args.create_env:
        create_env_template()
        return
    
    # Parse da data se fornecida
    target_date = None
    if args.date:
        try:
            target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        except ValueError:
            print("Data inválida. Use o formato YYYY-MM-DD")
            sys.exit(1)
    
    try:
        if not initialize_environment():
            print("Falha na inicialização do ambiente")
            sys.exit(1)
        
        pipeline = B3DataPipeline(
            skip_download=args.skip_download,
            skip_upload=args.skip_upload
        )
        
        success = pipeline.run_pipeline(target_date)
        
        if args.cleanup and success:
            pipeline.cleanup_old_files()
        
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\nPipeline interrompido pelo usuário")
        sys.exit(1)
    except Exception as e:
        print(f"Erro fatal: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
