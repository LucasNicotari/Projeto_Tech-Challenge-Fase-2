# Projeto Tech Challenge - Pipeline de Dados B3

Pipeline automatizado para extração, processamento e armazenamento de dados do IBOVESPA da B3 (Brasil, Bolsa, Balcão).

## 📋 Descrição

Este projeto implementa um pipeline completo de dados que:
- Faz web scraping dos dados do IBOVESPA diretamente do site da B3
- Processa e limpa os dados extraídos
- Converte para formato Parquet otimizado para análise
- Faz upload automático para Amazon S3
- Inclui logging completo e tratamento de erros

## 🚀 Funcionalidades

- **Web Scraping Automatizado**: Extração de dados diretamente do site da B3
- **Processamento Inteligente**: Limpeza e formatação dos dados
- **Detecção de Dias Não Úteis**: Automaticamente baixa dados do próximo dia útil
- **Formato Otimizado**: Conversão para Parquet para melhor performance
- **Upload S3**: Armazenamento automático na nuvem AWS
- **Logging Completo**: Rastreamento detalhado de todas as operações
- **Tratamento de Erros**: Recuperação automática de falhas

## 🛠️ Tecnologias Utilizadas

- **Python 3.8+**
- **Selenium**: Web scraping automatizado
- **Pandas**: Manipulação e processamento de dados
- **PyArrow**: Geração de arquivos Parquet
- **Boto3**: Integração com AWS S3
- **Chrome WebDriver**: Automação do navegador

## 📦 Instalação

1. Clone o repositório:
```bash
git clone https://github.com/seu-usuario/Projeto-Tech-Challenge.git
cd Projeto-Tech-Challenge
```

2. Instale as dependências:
```bash
pip install -r requirements_file.txt
```

3. Configure as variáveis de ambiente:
```bash
cp .env.example .env
# Edite o arquivo .env com suas credenciais AWS
```

## ⚙️ Configuração

### Variáveis de Ambiente

Crie um arquivo `.env` com as seguintes variáveis:

```env
# AWS Configuration
AWS_ACCESS_KEY_ID=sua_access_key
AWS_SECRET_ACCESS_KEY=sua_secret_key
AWS_SESSION_TOKEN=seu_session_token  # Opcional
AWS_REGION=us-east-1
S3_BUCKET_NAME=seu-bucket-name

# Pipeline Settings
LOG_LEVEL=INFO
CLEANUP_DAYS=7
SELENIUM_TIMEOUT=20
```

### Pré-requisitos

- Chrome/Chromium instalado
- ChromeDriver no PATH do sistema
- Credenciais AWS configuradas
- Bucket S3 criado

## 🚀 Uso

### Execução Básica

```bash
# Executar pipeline para data atual
python main_pipeline.py

# Executar para data específica
python main_pipeline.py --date 2025-08-03

# Pular download se arquivo já existir
python main_pipeline.py --skip-download

# Pular upload para S3
python main_pipeline.py --skip-upload

# Limpar arquivos antigos após execução
python main_pipeline.py --cleanup
```

### Módulos Individuais

```bash
# Apenas fazer scraping
python scraper_module.py

# Apenas processar dados existentes
python data_processor_module.py

# Apenas fazer upload
python s3_uploader_module.py
```

## 📁 Estrutura do Projeto

```
Projeto-Tech-Challenge/
├── main_pipeline.py          # Pipeline principal
├── scraper_module.py         # Web scraping da B3
├── data_processor_module.py  # Processamento de dados
├── s3_uploader_module.py     # Upload para S3
├── config_module.py          # Configurações centralizadas
├── logger_module.py          # Sistema de logging
├── env_loader.py             # Carregamento de variáveis
├── requirements_file.txt     # Dependências Python
├── .gitignore               # Arquivos ignorados pelo Git
├── README.md                # Documentação
├── downloads/               # Arquivos CSV baixados
├── data/                    # Arquivos Parquet processados
└── logs/                    # Logs de execução
```

## 📊 Dados Processados

O pipeline processa os seguintes campos:

- **codigo**: Código da ação (ex: PETR4, VALE3)
- **acao**: Nome da empresa
- **tipo**: Tipo de ação (ON, PN, etc.)
- **qtdeteorica**: Quantidade teórica de ações
- **part**: Participação percentual no índice
- **data_pregao**: Data do pregão (formato dd/mm/aaaa)
- **timestamp_extracao**: Timestamp da extração (ISO format)

## 🔧 Configurações Avançadas

### Personalização de Caminhos S3

O arquivo é salvo em: `s3://bucket-name/raw/parquet file/arquivo.parquet`

### Configuração de Timeout

Ajuste o timeout do Selenium no arquivo `.env`:
```env
SELENIUM_TIMEOUT=30  # segundos
```

### Limpeza Automática

Configure quantos dias de arquivos manter:
```env
CLEANUP_DAYS=7  # manter arquivos por 7 dias
```

## 📝 Logs

Os logs são salvos em `logs/` com rotação diária:
- `B3DataPipeline_YYYYMMDD.log`: Log principal do pipeline
- `B3Scraper_YYYYMMDD.log`: Log do web scraping
- `DataProcessor_YYYYMMDD.log`: Log do processamento
- `S3Uploader_YYYYMMDD.log`: Log do upload S3

## 🤝 Contribuição

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo `LICENSE` para mais detalhes.

## 👨‍💻 Autor

Desenvolvido como parte do Tech Challenge - Pós-graduação FIAP

## 🆘 Suporte

Para suporte e dúvidas:
- Abra uma issue no GitHub
- Consulte os logs em `logs/` para diagnóstico
- Verifique a configuração das variáveis de ambiente
