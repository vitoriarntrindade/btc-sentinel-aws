# CryptoSentinel ETL 🚀

Pipeline de ETL para análise de sentimento em tempo real sobre Bitcoin combinando dados de mercado e sentimento de redes sociais.

## 📋 Visão Geral

O CryptoSentinel é um pipeline ETL moderno que:

- 📊 **Coleta preços** do Bitcoin em tempo real via CoinGecko API
- 📰 **Extrai posts** sobre Bitcoin de fontes públicas (RSS/APIs)
- 🧠 **Analisa sentimento** dos posts usando processamento de linguagem natural
- 📄 **Gera relatórios** estruturados em CSV com insights automáticos
- ⚡ **Executa localmente** para testes e validação

## 🛠️ Tecnologias Utilizadas

- **Python 3.10+**
- **Pandas** - Manipulação de dados
- **TextBlob** - Análise de sentimento (NLP)
- **Requests** - Coleta de dados via APIs
- **Pytest** - Testes unitários
- **Apache Airflow** - Orquestração (próximo passo)

## 📁 Estrutura do Projeto

```
airflow-project/
├── src/
│   └── crypto_etl.py          # Módulo principal ETL
├── tests/
│   ├── conftest.py            # Fixtures para testes
│   ├── test_crypto_etl.py     # Testes funcionais
│   └── test_crypto_etl_unit.py # Testes unitários
├── data/
│   ├── reports/               # Relatórios gerados
│   └── logs/                  # Logs de execução
├── config.py                  # Configurações do projeto
├── demo.py                    # Script de demonstração
├── pytest.ini                # Configuração do pytest
└── README.md                  # Este arquivo
```

## 🚀 Instalação e Configuração

### 1. Clonar e Configurar Ambiente

```bash
# Navegar para o diretório
cd /etlc-crypto-

# Ativar ambiente virtual (se configurado)
source .venv/bin/activate

# Instalar dependências
pip install pandas requests textblob pytest pytest-mock python-dotenv

# Baixar dados do TextBlob
python -c "import nltk; nltk.download('punkt'); nltk.download('brown')"
```

### 2. Estrutura de Dados

O pipeline cria automaticamente os diretórios necessários:
- `data/reports/` - Relatórios CSV gerados
- `data/logs/` - Logs de execução

## 🎯 Como Usar

### Execução Completa (Recomendado)

```bash
# Executar pipeline completo com demonstração
python demo.py
```

### Execução Programática

```python
from src.crypto_etl import run_crypto_etl_pipeline

# Executar pipeline
results = run_crypto_etl_pipeline()

if results['success']:
    print(f"✅ Sucesso! Preço BTC: ${results['btc_price']:,.2f}")
    print(f"🧠 Sentimento médio: {results['avg_sentiment']:.3f}")
    print(f"📄 Relatório: {results['report_path']}")
else:
    print(f"❌ Erro: {results['error']}")
```

### Componentes Individuais

```python
from src.crypto_etl import (
    CryptoPriceCollector,
    TwitterDataCollector,
    SentimentAnalyzer,
    ReportGenerator
)

# Coletar preço Bitcoin
price_collector = CryptoPriceCollector()
price_data = price_collector.get_bitcoin_price()

# Coletar posts sobre Bitcoin
twitter_collector = TwitterDataCollector()
posts = twitter_collector.collect_bitcoin_tweets_rss()

# Analisar sentimento
analyzer = SentimentAnalyzer()
sentiment_results = analyzer.analyze_batch_sentiment([post['text'] for post in posts])

# Gerar relatório
generator = ReportGenerator()
report_path = generator.generate_comprehensive_report(price_data, sentiment_results, posts)
```

## 🧪 Testes

### Executar Todos os Testes

```bash
# Testes unitários com pytest
python -m pytest tests/test_crypto_etl_unit.py -v

# Testes funcionais
python tests/test_crypto_etl.py --test all

# Teste individual
python tests/test_crypto_etl.py --test price
```

### Tipos de Teste

- **Testes Unitários** (`test_crypto_etl_unit.py`)
  - 24 testes cobrindo cada componente individualmente
  - Mocks para APIs externas
  - Fixtures reutilizáveis
  - Assertions claras e objetivas

- **Testes Funcionais** (`test_crypto_etl.py`)
  - Testes end-to-end com APIs reais
  - Validação do pipeline completo
  - Testes de integração

## 📊 Exemplo de Saída

### Relatório CSV Gerado
```csv
timestamp,btc_price_usd,sentiment_polarity,sentiment_label,tweet_text
2025-11-02T20:47:04,110407,0.8,Positivo,"Bitcoin reaches new highs today"
2025-11-02T20:47:04,110407,-0.3,Negativo,"Bitcoin crash concerns investors"
2025-11-02T20:47:04,110407,0.0,Neutro,"Bitcoin price analysis for today"
RESUMO_20251102_204704,110407,0.17,RESUMO,"Análise de 3 posts | Pos: 33.3% | Neg: 33.3%"
```

### Logs de Execução
```
2025-11-02 20:47:01 - INFO - 🚀 Iniciando pipeline CryptoSentinel ETL...
2025-11-02 20:47:01 - INFO - 🔍 Coletando preço atual do Bitcoin...
2025-11-02 20:47:01 - INFO - 💰 Preço Bitcoin: $110,407.00 USD
2025-11-02 20:47:03 - INFO - ✅ 7 posts coletados via RSS
2025-11-02 20:47:04 - INFO - 🧠 Analisando sentimento de 7 textos...
2025-11-02 20:47:04 - INFO - 📄 Relatório salvo: data/reports/crypto_sentinel_report_20251102_204704.csv
2025-11-02 20:47:04 - INFO - ✅ Pipeline CryptoSentinel executado com sucesso!
```

## 🔧 Configurações

### Arquivo `config.py`

- **APIs**: URLs e configurações de rate limiting
- **Fontes de Dados**: RSS feeds e APIs alternativas
- **Sentimento**: Thresholds para classificação
- **Relatórios**: Formatos e nomenclatura
- **Logging**: Configurações detalhadas

### Variáveis de Ambiente (Opcional)

```bash
# Para uso futuro com AWS
export AWS_S3_BUCKET="crypto-sentinel-reports"
export AWS_REGION="us-east-1"
export ENVIRONMENT="production"
```

## 📈 Métricas e Insights

O pipeline gera automaticamente:

- **Sentimento Médio**: Polaridade geral dos posts (-1 a +1)
- **Distribuição**: % de posts positivos, negativos e neutros
- **Correlação**: Relação entre sentimento e movimento de preços
- **Posts Destacados**: Exemplos mais positivos e negativos
- **Dados de Mercado**: Preço, volume, capitalização e variação 24h



## 🐛 Troubleshooting

### Problemas Comuns

**1. Erro SSL em RSS feeds**
```
SSLEOFError: EOF occurred in violation of protocol
```
- **Solução**: O pipeline usa fonte alternativa automaticamente

**2. Rate limiting de APIs**
```
HTTP 429 Too Many Requests
```
- **Solução**: Configurar delays em `config.py`

**3. Imports do TextBlob**
```
LookupError: Resource punkt not found
```
- **Solução**: Executar `python -c "import nltk; nltk.download('punkt')"`

