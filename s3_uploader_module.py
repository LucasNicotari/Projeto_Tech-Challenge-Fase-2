"""
Módulo para upload de arquivos para Amazon S3
"""
import boto3
from pathlib import Path
from typing import Optional, Dict, Any
from botocore.exceptions import ClientError, NoCredentialsError

from config_module import Config
from logger_module import get_logger

class S3Uploader:
    """Classe para upload de arquivos para S3"""
    
    def __init__(self):
        self.logger = get_logger('S3Uploader')
        self.s3_client = None
        self.aws_config = Config.get_aws_config()
        self._validate_aws_config()
    
    def _validate_aws_config(self) -> None:
        """Valida se as configurações AWS estão presentes"""
        required_fields = ['aws_access_key_id', 'aws_secret_access_key', 'bucket_name']
        missing_fields = [field for field in required_fields if not self.aws_config.get(field)]
        
        if missing_fields:
            self.logger.failure(f"Configurações AWS ausentes: {missing_fields}")
            raise ValueError(f"Configurações AWS obrigatórias ausentes: {missing_fields}")
        
        self.logger.success("Configurações AWS validadas")
    
    def _initialize_s3_client(self) -> None:
        """Inicializa o cliente S3"""
        try:
            self.logger.process(f"Inicializando cliente S3 para a região: {self.aws_config.get('region_name', 'default')}...")
            client_config = {
                'region_name': self.aws_config['region_name'],
                'aws_access_key_id': self.aws_config['aws_access_key_id'],
                'aws_secret_access_key': self.aws_config['aws_secret_access_key']
            }
            
            # Adiciona session token se disponível
            if self.aws_config.get('aws_session_token'):
                client_config['aws_session_token'] = self.aws_config['aws_session_token']
            
            self.s3_client = boto3.client('s3', **client_config)
            self.logger.success("Cliente S3 inicializado com sucesso")
            
        except Exception as e:
            self.logger.failure(f"Erro ao inicializar cliente S3: {e}")
            raise
    
    def test_bucket_access(self) -> bool:
        """
        Testa se o bucket está acessível
        
        Returns:
            bool: True se acessível, False caso contrário
        """
        if not self.s3_client:
            self._initialize_s3_client()
        
        try:
            bucket_name = self.aws_config['bucket_name']
            self.logger.process(f"Testando acesso ao bucket: '{bucket_name}'...")
            self.s3_client.head_bucket(Bucket=bucket_name)
            self.logger.success(f"Bucket '{bucket_name}' está acessível")
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                self.logger.failure(f"Bucket '{bucket_name}' não encontrado")
            elif error_code == '403':
                self.logger.failure(f"Sem permissão para acessar bucket '{bucket_name}'")
            else:
                self.logger.failure(f"Erro ao acessar bucket: {e}")
            return False
        except Exception as e:
            self.logger.failure(f"Erro inesperado ao testar bucket: {e}")
            return False
    
    def upload_file(self, file_path: Path, s3_key: Optional[str] = None, 
                   metadata: Optional[Dict[str, str]] = None) -> bool:
        """
        Faz upload de um arquivo para S3
        
        Args:
            file_path: Caminho do arquivo local
            s3_key: Chave do objeto no S3 (opcional, usa configuração padrão)
            metadata: Metadados do arquivo (opcional)
            
        Returns:
            bool: True se sucesso, False caso contrário
            
        Raises:
            FileNotFoundError: Se o arquivo não existir
            Exception: Para outros erros de upload
        """
        if not file_path.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
        
        if not self.s3_client:
            self._initialize_s3_client()
        
        # Usa chave padrão se não fornecida
        if s3_key is None:
            s3_key = Config.get_s3_key()
        
        bucket_name = self.aws_config['bucket_name']
        
        try:
            # Testa acesso ao bucket primeiro
            if not self.test_bucket_access():
                return False
            
            self.logger.process(f"Iniciando upload: {file_path} -> s3://{bucket_name}/{s3_key}")
            
            # Prepara argumentos do upload
            upload_args = {}
            if metadata:
                upload_args['Metadata'] = metadata
            
            # Adiciona metadados padrão
            default_metadata = {
                'source': 'b3-data-pipeline',
                'file_size': str(file_path.stat().st_size),
                'original_filename': file_path.name
            }
            
            if metadata:
                default_metadata.update(metadata)
            
            upload_args['Metadata'] = default_metadata
            self.logger.info(f"Metadados a serem enviados: {default_metadata}")
            
            # Faz o upload
            self.logger.process(f"Executando upload para s3://{bucket_name}/{s3_key}...")
            self.s3_client.upload_file(
                str(file_path), 
                bucket_name, 
                s3_key,
                ExtraArgs=upload_args
            )
            
            self.logger.success(f"Upload concluído: s3://{bucket_name}/{s3_key}")
            return True
            
        except NoCredentialsError:
            self.logger.failure("Credenciais AWS não encontradas ou inválidas")
            return False
        except ClientError as e:
            error_code = e.response['Error']['Code']
            self.logger.failure(f"Erro AWS ({error_code}): {e}")
            return False
        except Exception as e:
            self.logger.failure(f"Erro inesperado durante upload: {e}")
            return False
    
    def list_objects(self, prefix: str = '') -> list:
        """
        Lista objetos no bucket S3
        
        Args:
            prefix: Prefixo para filtrar objetos
            
        Returns:
            list: Lista de objetos encontrados
        """
        if not self.s3_client:
            self._initialize_s3_client()
        
        try:
            bucket_name = self.aws_config['bucket_name']
            response = self.s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix
            )
            
            objects = response.get('Contents', [])
            self.logger.info(f"Encontrados {len(objects)} objetos com prefixo '{prefix}'")
            return objects
            
        except Exception as e:
            self.logger.failure(f"Erro ao listar objetos: {e}")
            return []
    
    def get_object_info(self, s3_key: str) -> Optional[Dict[str, Any]]:
        """
        Obtém informações de um objeto no S3
        
        Args:
            s3_key: Chave do objeto no S3
            
        Returns:
            Dict: Informações do objeto ou None se não encontrado
        """
        if not self.s3_client:
            self._initialize_s3_client()
        
        try:
            bucket_name = self.aws_config['bucket_name']
            response = self.s3_client.head_object(Bucket=bucket_name, Key=s3_key)
            
            return {
                'size': response.get('ContentLength'),
                'last_modified': response.get('LastModified'),
                'etag': response.get('ETag'),
                'metadata': response.get('Metadata', {})
            }
            
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                self.logger.warning(f"Objeto não encontrado: {s3_key}")
            else:
                self.logger.failure(f"Erro ao obter info do objeto: {e}")
            return None
        except Exception as e:
            self.logger.failure(f"Erro inesperado: {e}")
            return None

def main():
    """Função principal para teste do módulo"""
    uploader = S3Uploader()
    
    # Exemplo de uso
    test_file = Config.DATA_DIR / Config.get_parquet_filename()
    
    try:
        if test_file.exists():
            success = uploader.upload_file(test_file)
            if success:
                print("Upload realizado com sucesso!")
            else:
                print("Falha no upload")
        else:
            print(f"Arquivo de teste não encontrado: {test_file}")
            
    except Exception as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    main()
