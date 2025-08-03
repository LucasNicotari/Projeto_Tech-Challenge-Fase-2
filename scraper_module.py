"""
Módulo para fazer web scraping dos dados da B3
"""
import time
import os
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

from config_module import Config
from logger_module import get_logger

class B3Scraper:
    """Classe para fazer scraping dos dados da B3"""
    
    def __init__(self):
        self.logger = get_logger('B3Scraper')
        self.driver = None
        Config.create_directories()
    
    def _setup_chrome_options(self) -> Options:
        """Configura as opções do Chrome"""
        chrome_options = Options()
        chrome_options.add_experimental_option("prefs", {
            "download.default_directory": str(Config.DOWNLOAD_DIR.absolute()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        })
        
        # Opções para headless (opcional)
        # chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        return chrome_options
    
    def _initialize_driver(self):
        """Inicializa o driver do Chrome"""
        try:
            chrome_options = self._setup_chrome_options()
            self.driver = webdriver.Chrome(options=chrome_options)
            self.logger.success("Driver Chrome inicializado com sucesso")
        except WebDriverException as e:
            self.logger.failure(f"Erro ao inicializar driver Chrome: {e}")
            raise
    
    def _wait_for_download_completion(self, expected_filename: str) -> tuple[bool, str]:
        """
        Aguarda a conclusão do download verificando se o arquivo existe
        
        Returns:
            tuple: (sucesso, nome_do_arquivo_baixado)
        """
        expected_path = Config.DOWNLOAD_DIR / expected_filename
        max_wait = 60  # 60 segundos máximo
        wait_interval = 2  # verifica a cada 2 segundos
        
        self.logger.process(f"Aguardando download de {expected_filename}...")
        
        # Lista de arquivos antes do download para comparação
        files_before = set(f.name for f in Config.DOWNLOAD_DIR.glob("IBOVDia_*.csv"))
        
        for _ in range(0, max_wait, wait_interval):
            # Verifica se o arquivo esperado existe
            if expected_path.exists() and expected_path.stat().st_size > 0:
                self.logger.success(f"Download concluído: {expected_path}")
                return True, expected_filename
            
            # Verifica se algum novo arquivo IBOV foi baixado (para casos de dias não úteis)
            files_after = set(f.name for f in Config.DOWNLOAD_DIR.glob("IBOVDia_*.csv"))
            new_files = files_after - files_before
            
            if new_files:
                # Pega o arquivo mais recente entre os novos
                newest_file = max(new_files, key=lambda f: (Config.DOWNLOAD_DIR / f).stat().st_mtime)
                newest_path = Config.DOWNLOAD_DIR / newest_file
                
                if newest_path.stat().st_size > 0:
                    self.logger.warning(f"Arquivo baixado com nome diferente: {newest_file}")
                    self.logger.info("Isso pode indicar que a data solicitada não é um dia útil")
                    return True, newest_file
            
            time.sleep(wait_interval)
        
        self.logger.failure(f"Timeout: Download não foi concluído em {max_wait} segundos")
        return False, ""
    
    def download_ibov_data(self, target_date=None) -> Path:
        """
        Faz o download dos dados do IBOVESPA
        
        Args:
            target_date: Data alvo para o download (opcional)
        
        Returns:
            Path: Caminho do arquivo baixado
        
        Raises:
            Exception: Se houver erro no download
        """
        expected_filename = Config.get_csv_filename(target_date)
        expected_path = Config.DOWNLOAD_DIR / expected_filename
        
        try:
            self._initialize_driver()
            
            self.logger.process(f"Acessando URL: {Config.B3_URL}")
            self.driver.get(Config.B3_URL)
            
            # Aguarda o botão de download estar presente e clicável
            wait = WebDriverWait(self.driver, Config.SELENIUM_TIMEOUT)
            download_button = wait.until(
                EC.element_to_be_clickable((By.XPATH, Config.DOWNLOAD_XPATH))
            )
            
            self.logger.process("Clicando no botão de download...")
            download_button.click()
            
            # Aguarda o download ser concluído
            success, actual_filename = self._wait_for_download_completion(expected_filename)
            if success:
                actual_path = Config.DOWNLOAD_DIR / actual_filename
                if actual_filename != expected_filename:
                    self.logger.info(f"Arquivo baixado: {actual_filename} (esperado: {expected_filename})")
                return actual_path
            else:
                raise Exception("Download não foi concluído no tempo esperado")
                
        except TimeoutException:
            self.logger.failure("Timeout: Elemento de download não encontrado")
            raise
        except Exception as e:
            self.logger.failure(f"Erro durante o scraping: {e}")
            raise
        finally:
            self._cleanup()
    
    def _cleanup(self):
        """Limpa recursos do driver"""
        if self.driver:
            try:
                self.driver.quit()
                self.logger.info("Driver fechado com sucesso")
            except Exception as e:
                self.logger.warning(f"Erro ao fechar driver: {e}")

def main():
    """Função principal para teste do módulo"""
    scraper = B3Scraper()
    try:
        file_path = scraper.download_ibov_data()
        print(f"Arquivo baixado em: {file_path}")
    except Exception as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    main()
