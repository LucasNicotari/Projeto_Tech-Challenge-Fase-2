"""Módulo para processamento e limpeza dos dados da B3"""
import pandas as pd
import unidecode
import re
from pathlib import Path
from typing import Optional, Dict, Any

from config_module import Config
from logger_module import get_logger

class DataProcessor:
    def __init__(self):
        self.logger = get_logger('DataProcessor')
        Config.create_directories()

    def clean_column_name(self, col_name: str) -> str:
        if not col_name or pd.isna(col_name):
            return 'unnamed_column'
        col_name = unidecode.unidecode(str(col_name))
        col_name = re.sub(r'[^a-zA-Z0-9_]', '', col_name)
        return col_name.lower() if col_name else 'unnamed_column'

    def validate_dataframe(self, df: pd.DataFrame) -> bool:
        if df.empty:
            self.logger.failure("DataFrame está vazio")
            return False
        missing_cols = set(Config.REQUIRED_COLUMNS) - set(df.columns)
        if missing_cols:
            self.logger.failure(f"Colunas obrigatórias ausentes: {missing_cols}")
            return False
        self.logger.success("DataFrame validado com sucesso")
        return True

    def extract_date_from_header(self, csv_path: Path) -> str:
        """
        Extrai a data do pregão do cabeçalho do arquivo CSV
        
        Args:
            csv_path: Caminho do arquivo CSV
            
        Returns:
            str: Data no formato dd/mm/aaaa
        """
        try:
            with open(csv_path, 'r', encoding='latin1') as file:
                first_line = file.readline().strip()
                
            # Procura por padrão de data no cabeçalho (ex: "Carteira do Dia 04/08/25")
            import re
            date_pattern = r'(\d{2})/(\d{2})/(\d{2})'
            match = re.search(date_pattern, first_line)
            
            if match:
                day, month, year = match.groups()
                # Converte ano de 2 dígitos para 4 dígitos (assumindo 20xx)
                full_year = f"20{year}"
                date_str = f"{day}/{month}/{full_year}"
                self.logger.info(f"Data do pregão extraída do cabeçalho: {date_str}")
                return date_str
            else:
                # Se não encontrar, usa data atual
                from datetime import datetime
                current_date = datetime.now().strftime("%d/%m/%Y")
                self.logger.warning(f"Data não encontrada no cabeçalho, usando data atual: {current_date}")
                return current_date
                
        except Exception as e:
            self.logger.warning(f"Erro ao extrair data do cabeçalho: {e}")
            from datetime import datetime
            return datetime.now().strftime("%d/%m/%Y")

    def clean_numeric_field(self, series: pd.Series, is_currency: bool = True) -> pd.Series:
        if series.empty:
            return series
        
        # Converte a série para string para garantir que os métodos .str funcionem
        cleaned = series.astype(str).str.strip()

        # Remove caracteres não numéricos comuns que podem interferir
        cleaned = cleaned.str.replace('%', '', regex=False)
        cleaned = cleaned.str.replace('-', '', regex=False)
        cleaned = cleaned.str.replace('nan', '', regex=False)
        cleaned = cleaned.str.replace('N/A', '', regex=False)

        # Para 'qtdeteorica', remove o ponto de milhar
        if is_currency:
            cleaned = cleaned.str.replace('.', '', regex=False)

        # Converte a vírgula decimal para ponto decimal
        cleaned = cleaned.str.replace(',', '.', regex=False)

        # Converte a série limpa para formato numérico, tratando erros
        return pd.to_numeric(cleaned, errors='coerce')

    def process_csv_to_dataframe(self, csv_path: Path) -> pd.DataFrame:
        if not csv_path.exists():
            self.logger.failure(f"Arquivo CSV não encontrado: {csv_path}")
            raise FileNotFoundError(f"Arquivo CSV não encontrado: {csv_path}")
            
        self.logger.process(f"Lendo CSV: {csv_path}")
        try:
            # Primeiro, detecta o número de colunas no arquivo
            sample_df = pd.read_csv(
                csv_path,
                encoding='latin1',
                delimiter=';',
                skiprows=1,
                header=None,
                nrows=1,
                engine='python'
            )
            
            num_cols = len(sample_df.columns)
            self.logger.info(f"Detectadas {num_cols} colunas no arquivo CSV")
            
            # Define nomes de colunas baseado no número detectado
            if num_cols >= 7:
                # Formato com data e timestamp
                column_names = ['codigo', 'acao', 'tipo', 'qtdeteorica', 'part', 'data_pregao', 'timestamp_extracao']
                usecols = range(7)
            elif num_cols >= 5:
                # Formato básico (sem data e timestamp)
                column_names = ['codigo', 'acao', 'tipo', 'qtdeteorica', 'part']
                usecols = range(5)
            else:
                raise ValueError(f"Arquivo CSV tem formato inválido: apenas {num_cols} colunas encontradas")

            # Leitura do CSV com o engine python para maior robustez.
            df = pd.read_csv(
                csv_path,
                encoding='latin1',
                delimiter=';',
                skiprows=1,
                header=None,
                names=column_names,
                usecols=usecols,
                engine='python',
                on_bad_lines='skip'
            )

            self.logger.info(f"Arquivo CSV lido com sucesso. Colunas carregadas: {df.columns.tolist()}")
            
            # Remove linhas que possam ter sido lidas como totalmente vazias
            df.dropna(how='all', inplace=True)

            # Limpa a coluna 'codigo'
            if 'codigo' in df.columns:
                df['codigo'] = df['codigo'].str.strip().str.upper()

            self.logger.process("Limpando e convertendo campos numéricos...")
            
            # Limpa e converte as duas colunas numéricas
            df['qtdeteorica'] = self.clean_numeric_field(df['qtdeteorica'], is_currency=True)
            df['part'] = self.clean_numeric_field(df['part'], is_currency=False)

            # Adiciona colunas de data e timestamp se não existirem
            if 'data_pregao' not in df.columns:
                # Extrai a data do pregão do cabeçalho do arquivo
                data_pregao_str = self.extract_date_from_header(csv_path)
                df['data_pregao'] = data_pregao_str
                self.logger.info(f"Coluna 'data_pregao' adicionada: {data_pregao_str}")
            
            if 'timestamp_extracao' not in df.columns:
                # Adiciona timestamp de extração no formato ISO (YYYY-MM-DDTHH:MM:SS)
                from datetime import datetime
                timestamp_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                df['timestamp_extracao'] = timestamp_str
                self.logger.info(f"Coluna 'timestamp_extracao' adicionada: {timestamp_str}")

            self.logger.process("Formatando campos de data e hora...")
            # Converte data_pregao do formato dd/mm/aaaa para datetime
            df['data_pregao'] = pd.to_datetime(df['data_pregao'], format='%d/%m/%Y', errors='coerce')
            # Converte timestamp_extracao do formato ISO para datetime
            df['timestamp_extracao'] = pd.to_datetime(df['timestamp_extracao'], errors='coerce')
            
            # Limpeza final e remoção de nulos em colunas essenciais.
            df.dropna(subset=['codigo', 'qtdeteorica', 'part'], inplace=True)

            if not self.validate_dataframe(df):
                raise ValueError("DataFrame final não passou na validação.")

            self.logger.success(f"Processamento concluído: {len(df)} linhas válidas")
            return df
            
        except FileNotFoundError as e:
            self.logger.failure(str(e))
            raise
        except Exception as e:
            self.logger.failure(f"Erro inesperado ao processar CSV: {e}")
            raise

    def save_to_parquet(self, df: pd.DataFrame, output_path: Path) -> None:
        try:
            self.logger.process(f"Salvando DataFrame em Parquet: {output_path}")
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Converte colunas de data/hora para string no formato YYYY-MM-DD para máxima compatibilidade
            self.logger.process("Convertendo timestamps para string para compatibilidade...")
            df_copy = df.copy()
            df_copy['data_pregao'] = df_copy['data_pregao'].dt.strftime('%Y-%m-%d')
            df_copy['timestamp_extracao'] = df_copy['timestamp_extracao'].dt.strftime('%Y-%m-%d')

            df_copy.to_parquet(
                output_path, 
                index=False, 
                engine='pyarrow',
                version='1.0',
                compression='snappy'
            )
            file_size = output_path.stat().st_size / 1024
            self.logger.success(f"Arquivo Parquet salvo com sucesso: {output_path} ({file_size:.1f} KB)")
        except Exception as e:
            self.logger.failure(f"Erro ao salvar arquivo Parquet: {e}")
            raise

    def get_data_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        return {
            'total_rows': len(df),
            'total_companies': df['codigo'].nunique(),
            'total_theoretical_quantity': df['qtdeteorica'].sum(),
            'average_participation': df['part'].mean(),
            'top_5_companies': df.nlargest(5, 'part')[['codigo', 'acao', 'part']].to_dict('records')
        }

def main():
    processor = DataProcessor()
    
    # Busca os nomes de arquivo a partir do módulo de configuração
    try:
        csv_file = Config.DOWNLOAD_DIR / Config.get_csv_filename()
        parquet_file = Config.DATA_DIR / Config.get_parquet_filename()
    except Exception as e:
        get_logger('Main').failure(f"Erro ao obter configuração de arquivos: {e}")
        return

    # Executa o pipeline de processamento
    try:
        df = processor.process_csv_to_dataframe(csv_file)
        if df is not None and not df.empty:
            processor.save_to_parquet(df, parquet_file)
            summary = processor.get_data_summary(df)
            print("\n--- Resumo dos Dados Processados ---")
            print(summary)
            print("------------------------------------")
    except Exception as e:
        get_logger('Main').failure(f"Ocorreu um erro no pipeline principal: {e}")

if __name__ == "__main__":
    main()
