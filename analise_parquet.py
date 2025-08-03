import pandas as pd
import sys

def analisar_parquet(file_path):
    """
    Lê um arquivo Parquet e exibe seu cabeçalho e informações.
    """
    try:
        print(f"Analisando o arquivo: {file_path}")
        df = pd.read_parquet(file_path)
        
        print("\n--- Cabeçalho e Primeiras Linhas ---")
        print(df.head())
        
        print("\n--- Informações do DataFrame (Tipos de Dados e Nulos) ---")
        df.info()
        
        print("\n--- Análise Concluída ---")
        
    except FileNotFoundError:
        print(f"ERRO: O arquivo '{file_path}' não foi encontrado.")
        sys.exit(1)
    except Exception as e:
        print(f"ERRO: Ocorreu um erro ao ler o arquivo Parquet: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # O nome do arquivo é passado como argumento, ou usa o padrão
    if len(sys.argv) > 1:
        file_to_analyze = sys.argv[1]
    else:
        file_to_analyze = "IBOVDia_01-08-25.parquet"
        
    analisar_parquet(file_to_analyze)
