"""Script para verificar as colunas criadas no arquivo Parquet"""
import pandas as pd
from pathlib import Path

# Carrega o arquivo Parquet mais recente
parquet_file = Path("data/IBOVDia_03-08-25.parquet")

if parquet_file.exists():
    df = pd.read_parquet(parquet_file)
    
    print("🔍 VERIFICAÇÃO DAS COLUNAS CRIADAS")
    print("=" * 50)
    print(f"📊 Total de linhas: {len(df)}")
    print(f"📋 Colunas disponíveis: {list(df.columns)}")
    print()
    
    # Mostra as primeiras 3 linhas com foco nas novas colunas
    print("📅 AMOSTRAS DAS NOVAS COLUNAS:")
    print("-" * 50)
    sample_data = df[['codigo', 'data_pregao', 'timestamp_extracao']].head(3)
    for idx, row in sample_data.iterrows():
        print(f"Código: {row['codigo']}")
        print(f"Data Pregão: {row['data_pregao']}")
        print(f"Timestamp Extração: {row['timestamp_extracao']}")
        print("-" * 30)
    
    print("✅ Verificação concluída!")
else:
    print("❌ Arquivo Parquet não encontrado!")
