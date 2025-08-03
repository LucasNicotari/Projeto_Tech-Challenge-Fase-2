"""
Exemplo de testes para o pipeline B3
"""
import unittest
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, patch
import tempfile
import shutil

from config import Config
from data_processor import DataProcessor
from logger_utils import get_logger

class TestDataProcessor(unittest.TestCase):
    """Testes para o processador de dados"""
    
    def setUp(self):
        """Configuração inicial dos testes"""
        self.processor = DataProcessor()
        self.temp_dir = Path(tempfile.mkdtemp())
        
    def tearDown(self):
        """Limpeza após os testes"""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def test_clean_column_name(self):
        """Testa limpeza de nomes de colunas"""

        test_cases = [
            ("Código da Ação", "codigodaacao"),
            ("Part. (%)", "part"),
            ("TESTE_123", "teste_123"),
            ("", "unnamed_column"),
            (None, "unnamed_column")
        ]
        
        for input_name, expected in test_cases:
            with self.subTest(input_name=input_name):
                result = self.processor.clean_column_name(input_name)
                self.assertEqual(result, expected)
    
    def test_clean_numeric_field(self):
        """Testa limpeza de campos numéricos"""

        test_data = pd.Series(["1.234,56", "999,99", "invalid", ""])
        
        result = self.processor.clean_numeric_field(test_data, is_currency=True)
        
        self.assertAlmostEqual(result.iloc[0], 1234.56)
        self.assertAlmostEqual(result.iloc[1], 999.99)
        self.assertTrue(pd.isna(result.iloc[2])) 
    
    def test_filter_valid_rows(self):
        """Testa filtro de linhas válidas"""

        test_df = pd.DataFrame({
            'codigo': ['PETR4', 'VALE3', 'invalid123', '', 'ITUB4'],
            'acao': ['PETROBRAS', 'VALE', 'INVALID', '', 'ITAU'],
            'tipo': ['ON', 'ON', 'ON', '', 'PN'],
            'qtdeteorica': ['100', '200', '300', '', '400'],
            'part': ['5.5', '3.2', '1.1', '', '4.8']
        })
        
        result = self.processor.filter_valid_rows(test_df)
        
        self.assertEqual(len(result), 3)
        self.assertTrue(all(result['codigo'].str.match(Config.CODIGO_PATTERN)))
    
    def create_test_csv(self) -> Path:
        """Cria arquivo CSV de teste"""
        csv_content = """Data de Referência: 15/01/2024
Código;Ação;Tipo;Qtde. Teórica;Part. (%)
PETR4;PETROBRAS PN;PN;1.234.567,89;5,5
VALE3;VALE ON;ON;987.654,32;3,2
ITUB4;ITAU UNIBANCO PN;PN;2.345.678,90;4,8
Total;;;4.567.901,11;13,5
"""
        
        csv_path = self.temp_dir / "test_ibov.csv"
        csv_path.write_text(csv_content, encoding='latin1')
        return csv_path
    
    def test_process_csv_to_dataframe(self):
        """Testa processamento completo do CSV"""

        csv_path = self.create_test_csv()
        
        result_df = self.processor.process_csv_to_dataframe(csv_path)
        
        self.assertEqual(len(result_df), 3)  # 3 ações válidas
        self.assertTrue(all(col in result_df.columns for col in ['codigo', 'acao', 'tipo', 'qtdeteorica', 'part']))
        
        self.assertTrue(pd.api.types.is_numeric_dtype(result_df['qtdeteorica']))
        self.assertTrue(pd.api.types.is_numeric_dtype(result_df['part']))
        
        petr4_row = result_df[result_df['codigo'] == 'PETR4'].iloc[0]
        self.assertAlmostEqual(petr4_row['qtdeteorica'], 1234567.89)
        self.assertAlmostEqual(petr4_row['part'], 5.5)

class TestConfig(unittest.TestCase):
    """Testes para configurações"""
    
    def test_get_csv_filename(self):
        """Testa geração de nome de arquivo CSV"""
        from datetime import date
        
        test_date = date(2024, 1, 15)
        filename = Config.get_csv_filename(test_date)
        
        self.assertEqual(filename, "IBOVDia_15-01-24.csv")
    
    def test_get_parquet_filename(self):
        """Testa geração de nome de arquivo Parquet"""
        from datetime import date
        
        test_date = date(2024, 1, 15) 
        filename = Config.get_parquet_filename(test_date)
        
        self.assertEqual(filename, "IBOVDia_15-01-24.parquet")
    
    def test_get_s3_key(self):
        """Testa geração de chave S3"""
        from datetime import date
        
        test_date = date(2024, 1, 15)
        s3_key = Config.get_s3_key(test_date)
        
        expected = "raw/date=2024-01-15/IBOVDia_15-01-24.parquet"
        self.assertEqual(s3_key, expected)

class TestLogger(unittest.TestCase):
    """Testes para sistema de logging"""
    
    def test_logger_creation(self):
        """Testa criação de logger"""
        logger = get_logger('test_logger')
        
        self.assertIsNotNone(logger)
        self.assertEqual(logger.name, 'test_logger')
    
    def test_log_methods(self):
        """Testa métodos de log"""
        logger = get_logger('test_methods')
        
        try:
            logger.info("Test info")
            logger.error("Test error") 
            logger.warning("Test warning")
            logger.success("Test success")
            logger.process("Test process")
            logger.failure("Test failure")
        except Exception as e:
            self.fail(f"Log methods failed: {e}")

def run_integration_test():
    """Teste de integração simplificado"""
    print("🧪 EXECUTANDO TESTES DE INTEGRAÇÃO")
    print("=" * 50)
    
    try:
        print("📦 Testando importações dos módulos...")
        from config import Config
        from logger_utils import get_logger
        from data_processor import DataProcessor
        print("   ✅ Todas as importações funcionaram")
        
        print("🔧 Testando configurações...")
        filename = Config.get_csv_filename()
        assert filename.endswith('.csv')
        print(f"   ✅ Geração de filename: {filename}")
        
        print("📝 Testando sistema de logging...")
        logger = get_logger('integration_test')
        logger.info("Teste de integração")
        print("   ✅ Logger funcionando")
        
        print("⚙️  Testando processador de dados...")
        processor = DataProcessor()
        
        clean_name = processor.clean_column_name("Teste Ação")
        assert clean_name == "testeacao"
        print("   ✅ Limpeza de nomes funcionando")
        
        print("\n✅ TODOS OS TESTES DE INTEGRAÇÃO PASSARAM!")
        return True
        
    except Exception as e:
        print(f"\n❌ TESTE DE INTEGRAÇÃO FALHOU: {e}")
        return False

if __name__ == "__main__":
    print("🧪 EXECUTANDO TESTES UNITÁRIOS")
    print("=" * 50)
    
    unittest.main(argv=[''], exit=False, verbosity=2)
    
    print("\n" + "=" * 50)
    
    run_integration_test()
