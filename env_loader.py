"""
Carregador de variáveis de ambiente para o projeto B3 Data Pipeline
"""
import os
from pathlib import Path
from typing import Optional, Dict, Any

def load_env_file(env_path: Optional[Path] = None) -> bool:
    if env_path is None:
        env_path = Path('.env')
    
    if not env_path.exists():
        print(f"Arquivo .env não encontrado: {env_path}")
        return False
    
    try:
        with open(env_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                
                if not line or line.startswith('#'):
                    continue
                
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    
                    if value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                    elif value.startswith("'") and value.endswith("'"):
                        value = value[1:-1]
                    
                    if key and not os.getenv(key):
                        os.environ[key] = value
                else:
                    print(f"Linha inválida no .env:{line_num}: {line}")
        
        print(f"Variáveis de ambiente carregadas de: {env_path}")
        return True
        
    except Exception as e:
        print(f"Erro ao carregar .env: {e}")
        return False

def validate_required_env_vars(required_vars: list) -> Dict[str, Any]:
    """
    Valida se todas as variáveis obrigatórias estão definidas
    
    Args:
        required_vars: Lista de variáveis obrigatórias
        
    Returns:
        Dict: Dicionário com status da validação
    """
    missing_vars = []
    empty_vars = []
    valid_vars = []
    
    for var in required_vars:
        value = os.getenv(var)
        if value is None:
            missing_vars.append(var)
        elif not value.strip():
            empty_vars.append(var)
        else:
            valid_vars.append(var)
    
    return {
        'valid': len(missing_vars) == 0 and len(empty_vars) == 0,
        'missing': missing_vars,
        'empty': empty_vars,
        'valid_vars': valid_vars,
        'total_required': len(required_vars),
        'total_valid': len(valid_vars)
    }

def get_aws_config_from_env() -> Dict[str, Optional[str]]:
    """
    Obtém configurações AWS das variáveis de ambiente
    
    Returns:
        Dict: Configurações AWS
    """
    return {
        'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
        'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
        'aws_session_token': os.getenv('AWS_SESSION_TOKEN'),
        'region_name': os.getenv('AWS_REGION', 'us-east-1'),
        'bucket_name': os.getenv('S3_BUCKET_NAME')
    }

def print_env_status():
    aws_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'S3_BUCKET_NAME']
    validation = validate_required_env_vars(aws_vars)
    
    print(f"\nSTATUS DAS VARIÁVEIS DE AMBIENTE:")
    print(f"   Válidas: {validation['total_valid']}/{validation['total_required']}")
    
    if validation['missing']:
        print(f"   Ausentes: {', '.join(validation['missing'])}")
    
    if validation['empty']:
        print(f"   Vazias: {', '.join(validation['empty'])}")
    
    optional_vars = {
        'AWS_SESSION_TOKEN': os.getenv('AWS_SESSION_TOKEN'),
        'AWS_REGION': os.getenv('AWS_REGION', 'us-east-1'),
        'LOG_LEVEL': os.getenv('LOG_LEVEL', 'INFO')
    }
    
    print("   Opcionais:")
    for var, value in optional_vars.items():
        status = "✓" if value and value not in ['us-east-1', 'INFO'] else "○"
        print(f"      {status} {var}: {value or 'não definida'}")

def initialize_environment() -> bool:
    print("Inicializando ambiente...")
    
    env_loaded = load_env_file()
    
    required_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'S3_BUCKET_NAME']
    validation = validate_required_env_vars(required_vars)
    
    print_env_status()
    
    if not validation['valid']:
        print("\nCONFIGURAÇÃO INCOMPLETA!")
        print("Configure as variáveis ausentes no arquivo .env")
        return False
    
    print("\nAmbiente configurado corretamente!")
    return True

# Carrega automaticamente quando o módulo é importado
if __name__ != "__main__":
    load_env_file()
