"""
Arquivo de configuração para o projeto CryptoSentinel ETL.

Este arquivo centraliza todas as configurações necessárias para o pipeline,
facilitando a manutenção e deploy em diferentes ambientes.

Autor: Vitória
Data: 2025-11-02
"""

import os
from datetime import timedelta
from pathlib import Path

# ==========================================
# CONFIGURAÇÕES GERAIS
# ==========================================

PROJECT_NAME = "CryptoSentinel"
VERSION = "1.0.0"
AUTHOR = "Vitória"

# Diretórios base
BASE_DIR = Path(__file__).parent
SRC_DIR = BASE_DIR / "src"
DATA_DIR = BASE_DIR / "data"
REPORTS_DIR = DATA_DIR / "reports"
LOGS_DIR = DATA_DIR / "logs"

# Criar diretórios se não existem
for directory in [DATA_DIR, REPORTS_DIR, LOGS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# ==========================================
# CONFIGURAÇÕES DE APIs
# ==========================================

# CoinGecko API
COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
COINGECKO_RATE_LIMIT = 30  # requests per minute

# Fontes de dados RSS/Notícias
RSS_SOURCES = [
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=BTC-USD&region=US&lang=en-US",
    # "https://rss.cnn.com/rss/money_latest.rss",  # Desabilitado devido a problemas SSL
]

# API alternativa de notícias crypto
CRYPTO_NEWS_API = "https://min-api.cryptocompare.com/data/v2/news/"

# ==========================================
# CONFIGURAÇÕES DE COLETA
# ==========================================

# Limites de coleta
MAX_TWEETS_PER_SOURCE = 20
MAX_NEWS_ARTICLES = 15
REQUEST_TIMEOUT = 15  # segundos

# Rate limiting
REQUESTS_DELAY = 1  # segundo entre requests
MAX_RETRIES = 3

# ==========================================
# CONFIGURAÇÕES DE SENTIMENTO
# ==========================================

# Thresholds para classificação de sentimento
POSITIVE_THRESHOLD = 0.1
NEGATIVE_THRESHOLD = -0.1

# Configurações TextBlob
TEXTBLOB_REQUIRED_CORPORA = ["punkt", "brown"]

# ==========================================
# CONFIGURAÇÕES DE RELATÓRIOS
# ==========================================

# Formatos de arquivo
REPORT_FORMAT = "csv"
REPORT_ENCODING = "utf-8"

# Padrões de nomenclatura
REPORT_FILENAME_PATTERN = "crypto_sentinel_report_{timestamp}.csv"
TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"

# ==========================================
# CONFIGURAÇÕES AIRFLOW DAG
# ==========================================

# DAG básicas
DAG_ID = "crypto_sentinel_etl"
DAG_DESCRIPTION = "Pipeline ETL para análise de sentimento Bitcoin com dados em tempo real"

# Schedule
DAG_SCHEDULE_INTERVAL = "*/15 * * * *"  # A cada 15 minutos
DAG_START_DATE_YEAR = 2025
DAG_START_DATE_MONTH = 11
DAG_START_DATE_DAY = 2

# Configurações de retry
DAG_RETRIES = 2
DAG_RETRY_DELAY = timedelta(minutes=5)

# Tags
DAG_TAGS = ["etl", "crypto", "bitcoin", "sentiment", "nlp", "finance"]

# ==========================================
# CONFIGURAÇÕES DE LOGGING
# ==========================================

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        },
        'detailed': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s'
        }
    },
    'handlers': {
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'standard'
        },
        'file': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'filename': str(LOGS_DIR / 'crypto_sentinel.log'),
            'formatter': 'detailed',
            'mode': 'a'
        }
    },
    'loggers': {
        '': {  # root logger
            'handlers': ['console', 'file'],
            'level': 'INFO',
            'propagate': False
        }
    }
}

# ==========================================
# CONFIGURAÇÕES AWS (para futuro uso)
# ==========================================

# S3 Configuration (será usado quando integrarmos com AWS)
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET", "crypto-pulse-reports")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_S3_PREFIX = "reports/"

# ==========================================
# CONFIGURAÇÕES DE AMBIENTE
# ==========================================

def get_environment():
    """Detecta o ambiente atual (local, staging, production)."""
    return os.getenv("ENVIRONMENT", "local")

def is_production():
    """Verifica se está em produção."""
    return get_environment().lower() == "production"

def is_local():
    """Verifica se está em ambiente local."""
    return get_environment().lower() == "local"

# ==========================================
# VALIDAÇÕES DE CONFIGURAÇÃO
# ==========================================

def validate_config():
    """
    Valida se todas as configurações necessárias estão presentes.
    
    Returns:
        bool: True se configuração é válida
    """
    required_dirs = [SRC_DIR, DATA_DIR, REPORTS_DIR]
    
    for directory in required_dirs:
        if not directory.exists():
            try:
                directory.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                print(f"❌ Erro ao criar diretório {directory}: {e}")
                return False
    
    return True

# ==========================================
# CONFIGURAÇÕES DE DESENVOLVIMENTO
# ==========================================

# Para testes locais
LOCAL_TEST_CONFIG = {
    "max_tweets": 10,
    "max_news": 5,
    "skip_rate_limiting": True,
    "verbose_logging": True
}

# ==========================================
# EXPORTAR CONFIGURAÇÕES PRINCIPAIS
# ==========================================

__all__ = [
    'PROJECT_NAME',
    'VERSION',
    'BASE_DIR',
    'SRC_DIR',
    'DATA_DIR',
    'REPORTS_DIR',
    'LOGS_DIR',
    'COINGECKO_BASE_URL',
    'RSS_SOURCES',
    'CRYPTO_NEWS_API',
    'MAX_TWEETS_PER_SOURCE',
    'MAX_NEWS_ARTICLES',
    'POSITIVE_THRESHOLD',
    'NEGATIVE_THRESHOLD',
    'DAG_ID',
    'DAG_SCHEDULE_INTERVAL',
    'DAG_TAGS',
    'LOGGING_CONFIG',
    'validate_config',
    'get_environment',
    'is_production',
    'is_local'
]

# Validar configuração na importação
if __name__ != "__main__":
    if not validate_config():
        raise RuntimeError("❌ Falha na validação da configuração!")