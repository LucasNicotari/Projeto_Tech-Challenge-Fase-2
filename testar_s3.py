"""Script para testar conectividade S3 e listar buckets disponíveis"""
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from env_loader import get_aws_config_from_env, load_env_file

def testar_conectividade_s3():
    """Testa conectividade com S3 e lista buckets disponíveis"""
    print("🔍 TESTANDO CONECTIVIDADE S3")
    print("=" * 50)
    
    # Carrega configurações
    load_env_file()
    aws_config = get_aws_config_from_env()
    
    try:
        # Inicializa cliente S3
        client_config = {
            'region_name': aws_config['region_name'],
            'aws_access_key_id': aws_config['aws_access_key_id'],
            'aws_secret_access_key': aws_config['aws_secret_access_key']
        }
        
        if aws_config.get('aws_session_token'):
            client_config['aws_session_token'] = aws_config['aws_session_token']
        
        s3_client = boto3.client('s3', **client_config)
        print("✅ Cliente S3 inicializado com sucesso")
        
        # Lista buckets disponíveis
        print("\n📦 BUCKETS DISPONÍVEIS:")
        print("-" * 30)
        
        response = s3_client.list_buckets()
        buckets = response.get('Buckets', [])
        
        if buckets:
            for i, bucket in enumerate(buckets, 1):
                bucket_name = bucket['Name']
                creation_date = bucket['CreationDate'].strftime('%Y-%m-%d %H:%M:%S')
                print(f"{i}. {bucket_name} (criado em: {creation_date})")
        else:
            print("❌ Nenhum bucket encontrado na sua conta AWS")
        
        # Testa o bucket configurado
        bucket_configurado = aws_config['bucket_name']
        print(f"\n🎯 TESTANDO BUCKET CONFIGURADO: '{bucket_configurado}'")
        print("-" * 50)
        
        try:
            s3_client.head_bucket(Bucket=bucket_configurado)
            print(f"✅ Bucket '{bucket_configurado}' existe e está acessível")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                print(f"❌ Bucket '{bucket_configurado}' NÃO EXISTE")
                print("💡 Sugestões:")
                print("   1. Crie o bucket no console AWS")
                print("   2. Ou altere S3_BUCKET_NAME no .env para um bucket existente")
            elif error_code == '403':
                print(f"❌ SEM PERMISSÃO para acessar '{bucket_configurado}'")
                print("💡 Verifique as permissões IAM da sua conta")
            else:
                print(f"❌ Erro desconhecido: {e}")
        
        return True
        
    except NoCredentialsError:
        print("❌ CREDENCIAIS AWS INVÁLIDAS")
        print("💡 Verifique as credenciais no arquivo .env")
        return False
    except Exception as e:
        print(f"❌ ERRO INESPERADO: {e}")
        return False

if __name__ == "__main__":
    testar_conectividade_s3()
