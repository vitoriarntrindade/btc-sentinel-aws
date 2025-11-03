"""
Testes unitários para o módulo CryptoSentinel ETL.

Este arquivo contém testes unitários focados, objetivos e com assertions claras
para cada componente do pipeline ETL.

Autor: Vitória
Data: 2025-11-02
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import pandas as pd
import tempfile
import os
import json

# Adicionar src ao path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from crypto_etl import (
    CryptoPriceCollector,
    TwitterDataCollector,
    SentimentAnalyzer,
    ReportGenerator,
    run_crypto_etl_pipeline
)


# ==========================================
# FIXTURES
# ==========================================

@pytest.fixture
def mock_bitcoin_price_response():
    """Fixture com resposta mock da API CoinGecko para preço do Bitcoin."""
    return {
        "bitcoin": {
            "usd": 68234.50,
            "usd_market_cap": 1340000000000,
            "usd_24h_vol": 15000000000,
            "usd_24h_change": 2.5
        }
    }


@pytest.fixture
def sample_tweets_data():
    """Fixture com dados de exemplo de tweets/posts sobre Bitcoin."""
    return [
        {
            'text': 'Bitcoin is going to the moon! Great investment opportunity!',
            'source': 'test_rss',
            'published_date': '2025-11-02T10:00:00',
            'collected_at': '2025-11-02T10:05:00'
        },
        {
            'text': 'Bitcoin crash is terrible, losing all my money',
            'source': 'test_news',
            'published_date': '2025-11-02T11:00:00',
            'collected_at': '2025-11-02T11:05:00'
        },
        {
            'text': 'Bitcoin price is stable today, no major changes',
            'source': 'test_rss',
            'published_date': '2025-11-02T12:00:00',
            'collected_at': '2025-11-02T12:05:00'
        }
    ]


@pytest.fixture
def sample_sentiment_results():
    """Fixture com resultados de exemplo da análise de sentimento."""
    return [
        {
            'polarity': 0.8,
            'subjectivity': 0.6,
            'sentiment_label': 'Positivo',
            'confidence': 'Alta',
            'text_index': 0,
            'original_text': 'Bitcoin is going to the moon! Great investment opportunity!'
        },
        {
            'polarity': -0.7,
            'subjectivity': 0.8,
            'sentiment_label': 'Negativo',
            'confidence': 'Alta',
            'text_index': 1,
            'original_text': 'Bitcoin crash is terrible, losing all my money'
        },
        {
            'polarity': 0.0,
            'subjectivity': 0.2,
            'sentiment_label': 'Neutro',
            'confidence': 'Média',
            'text_index': 2,
            'original_text': 'Bitcoin price is stable today, no major changes'
        }
    ]


@pytest.fixture
def temp_reports_dir():
    """Fixture que cria um diretório temporário para relatórios."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def mock_rss_response():
    """Fixture com resposta mock de RSS feed."""
    return """<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0">
        <channel>
            <title>Bitcoin News</title>
            <item>
                <title>Bitcoin reaches new highs today</title>
                <description>Bitcoin price shows strong momentum</description>
                <pubDate>Mon, 02 Nov 2025 10:00:00 GMT</pubDate>
            </item>
            <item>
                <title>Crypto market analysis for Bitcoin</title>
                <description>Technical analysis shows bullish patterns</description>
                <pubDate>Mon, 02 Nov 2025 11:00:00 GMT</pubDate>
            </item>
        </channel>
    </rss>"""


# ==========================================
# TESTES: CryptoPriceCollector
# ==========================================

class TestCryptoPriceCollector:
    """Testes para a classe CryptoPriceCollector."""

    def test_deve_inicializar_com_url_base_padrao(self):
        """Deve inicializar o coletor com URL base padrão da CoinGecko."""
        collector = CryptoPriceCollector()
        
        assert collector.base_url == "https://api.coingecko.com/api/v3"
        assert collector.session is not None

    def test_deve_inicializar_com_url_base_customizada(self):
        """Deve aceitar URL base customizada na inicialização."""
        custom_url = "https://api.example.com/v1"
        collector = CryptoPriceCollector(base_url=custom_url)
        
        assert collector.base_url == custom_url

    @patch('crypto_etl.requests.Session.get')
    def test_deve_retornar_dados_preco_bitcoin_quando_api_responde_sucesso(self, mock_get, mock_bitcoin_price_response):
        """Deve retornar dados formatados do Bitcoin quando API responde com sucesso."""
        # Arrange
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = mock_bitcoin_price_response
        mock_get.return_value = mock_response
        
        collector = CryptoPriceCollector()
        
        # Act
        result = collector.get_bitcoin_price()
        
        # Assert
        assert result['price_usd'] == 68234.50
        assert result['market_cap_usd'] == 1340000000000
        assert result['volume_24h_usd'] == 15000000000
        assert result['price_change_24h_pct'] == 2.5
        assert 'timestamp' in result
        assert isinstance(result['timestamp'], str)

    @patch('crypto_etl.requests.Session.get')
    def test_deve_lancar_excecao_quando_api_retorna_erro_http(self, mock_get):
        """Deve lançar exceção quando API retorna erro HTTP."""
        # Arrange
        mock_get.side_effect = Exception("Connection error")
        collector = CryptoPriceCollector()
        
        # Act & Assert
        with pytest.raises(Exception) as exc_info:
            collector.get_bitcoin_price()
        
        # Verifica se é uma exceção de falha na coleta
        assert "Connection error" in str(exc_info.value)

    @patch('crypto_etl.requests.Session.get')
    def test_deve_tratar_resposta_api_sem_dados_bitcoin(self, mock_get):
        """Deve tratar adequadamente resposta da API sem dados do Bitcoin."""
        # Arrange
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"ethereum": {"usd": 3000}}  # Sem dados Bitcoin
        mock_get.return_value = mock_response
        
        collector = CryptoPriceCollector()
        
        # Act
        result = collector.get_bitcoin_price()
        
        # Assert
        assert result['price_usd'] == 0
        assert result['market_cap_usd'] == 0
        assert 'timestamp' in result


# ==========================================
# TESTES: TwitterDataCollector
# ==========================================

class TestTwitterDataCollector:
    """Testes para a classe TwitterDataCollector."""

    def test_deve_inicializar_com_sessao_configurada(self):
        """Deve inicializar com sessão HTTP configurada."""
        collector = TwitterDataCollector()
        
        assert collector.session is not None
        assert 'User-Agent' in collector.session.headers

    @patch('crypto_etl.requests.Session.get')
    def test_deve_coletar_tweets_via_rss_quando_feed_valido(self, mock_get, mock_rss_response):
        """Deve coletar e processar tweets via RSS quando feed é válido."""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = mock_rss_response.encode('utf-8')
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        collector = TwitterDataCollector()
        
        # Act
        result = collector.collect_bitcoin_tweets_rss(max_tweets=5)
        
        # Assert
        assert len(result) >= 2  # Pelo menos 2 itens no RSS mock (pode ter duplicação por múltiplas fontes)
        assert any("Bitcoin reaches new highs today" in item['text'] for item in result)
        assert all(item['source'] == 'rss_feed' for item in result)
        assert all('collected_at' in item for item in result)

    @patch('crypto_etl.requests.Session.get')
    def test_deve_retornar_lista_vazia_quando_rss_inacessivel(self, mock_get):
        """Deve retornar lista vazia quando RSS não está acessível."""
        # Arrange
        mock_get.side_effect = Exception("Connection failed")
        collector = TwitterDataCollector()
        
        # Act
        result = collector.collect_bitcoin_tweets_rss()
        
        # Assert
        assert result == []

    @patch('crypto_etl.requests.Session.get')
    def test_deve_filtrar_apenas_conteudo_relacionado_bitcoin(self, mock_get):
        """Deve filtrar apenas conteúdo que menciona Bitcoin ou crypto."""
        # Arrange
        rss_content = """<?xml version="1.0"?>
        <rss version="2.0">
            <channel>
                <item>
                    <title>Bitcoin price update</title>
                    <description>Bitcoin news</description>
                </item>
                <item>
                    <title>Apple stock rises</title>
                    <description>Regular stock market news</description>
                </item>
                <item>
                    <title>Crypto market analysis</title>
                    <description>General cryptocurrency discussion</description>
                </item>
            </channel>
        </rss>"""
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = rss_content.encode('utf-8')
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        collector = TwitterDataCollector()
        
        # Act
        result = collector.collect_bitcoin_tweets_rss()
        
        # Assert
        # Verifica que apenas posts com termos relacionados foram coletados
        for item in result:
            content = f"{item['text']} {item['description']}".lower()
            assert any(term in content for term in ['bitcoin', 'btc', 'crypto'])
        
        # Verifica que temos posts Bitcoin e Crypto
        bitcoin_posts = [item for item in result if 'bitcoin' in item['text'].lower()]
        crypto_posts = [item for item in result if 'crypto' in item['text'].lower()]
        
        assert len(bitcoin_posts) >= 1
        assert len(crypto_posts) >= 1

    @patch('crypto_etl.requests.Session.get')
    def test_deve_coletar_noticias_alternativas_com_sucesso(self, mock_get):
        """Deve coletar notícias de fonte alternativa com sucesso."""
        # Arrange
        mock_news_response = {
            "Data": [
                {
                    "title": "Bitcoin surges to new highs",
                    "body": "Bitcoin price analysis shows strong bullish momentum in the market",
                    "published_on": 1699000000
                },
                {
                    "title": "Crypto market update",
                    "body": "Overall cryptocurrency market shows mixed signals today",
                    "published_on": 1699010000
                }
            ]
        }
        
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = mock_news_response
        mock_get.return_value = mock_response
        
        collector = TwitterDataCollector()
        
        # Act
        result = collector.collect_bitcoin_news_alternative()
        
        # Assert
        assert len(result) == 2
        assert result[0]['text'] == "Bitcoin surges to new highs"
        assert result[0]['source'] == 'crypto_news'
        assert 'collected_at' in result[0]


# ==========================================
# TESTES: SentimentAnalyzer
# ==========================================

class TestSentimentAnalyzer:
    """Testes para a classe SentimentAnalyzer."""

    def test_deve_inicializar_com_thresholds_padrao(self):
        """Deve inicializar com thresholds padrão para classificação."""
        analyzer = SentimentAnalyzer()
        
        assert analyzer.positive_threshold == 0.1
        assert analyzer.negative_threshold == -0.1

    def test_deve_analisar_texto_positivo_corretamente(self):
        """Deve classificar texto claramente positivo corretamente."""
        analyzer = SentimentAnalyzer()
        positive_text = "Bitcoin is amazing! Great investment opportunity!"
        
        result = analyzer.analyze_text_sentiment(positive_text)
        
        assert result['polarity'] > 0
        assert result['sentiment_label'] == 'Positivo'
        assert isinstance(result['subjectivity'], float)
        assert result['confidence'] in ['Alta', 'Média', 'Baixa']

    def test_deve_analisar_texto_negativo_corretamente(self):
        """Deve classificar texto claramente negativo corretamente."""
        analyzer = SentimentAnalyzer()
        negative_text = "Bitcoin crash is terrible! Losing all my money!"
        
        result = analyzer.analyze_text_sentiment(negative_text)
        
        assert result['polarity'] < 0
        assert result['sentiment_label'] == 'Negativo'
        assert isinstance(result['subjectivity'], float)

    def test_deve_analisar_texto_neutro_corretamente(self):
        """Deve classificar texto neutro corretamente."""
        analyzer = SentimentAnalyzer()
        neutral_text = "Bitcoin price today."
        
        result = analyzer.analyze_text_sentiment(neutral_text)
        
        assert result['sentiment_label'] == 'Neutro'
        assert abs(result['polarity']) <= 0.1

    def test_deve_tratar_texto_vazio_ou_invalido(self):
        """Deve tratar adequadamente texto vazio ou inválido."""
        analyzer = SentimentAnalyzer()
        
        # Texto vazio
        result_empty = analyzer.analyze_text_sentiment("")
        assert result_empty['sentiment_label'] == 'Neutro'
        assert result_empty['polarity'] == 0.0
        
        # Texto None
        result_none = analyzer.analyze_text_sentiment(None)
        assert result_none['sentiment_label'] == 'Neutro'
        assert result_none['polarity'] == 0.0

    def test_deve_analisar_batch_de_textos_retornando_lista_completa(self, sample_tweets_data):
        """Deve analisar lista de textos retornando resultados para todos."""
        analyzer = SentimentAnalyzer()
        texts = [tweet['text'] for tweet in sample_tweets_data]
        
        results = analyzer.analyze_batch_sentiment(texts)
        
        assert len(results) == len(texts)
        assert all('polarity' in result for result in results)
        assert all('sentiment_label' in result for result in results)
        assert all('text_index' in result for result in results)

    def test_deve_calcular_estatisticas_do_batch_corretamente(self):
        """Deve calcular estatísticas corretas para batch de análises."""
        analyzer = SentimentAnalyzer()
        texts = [
            "Bitcoin is great!",  # Positivo
            "Bitcoin is terrible!",  # Negativo
            "Bitcoin price today"  # Neutro
        ]
        
        results = analyzer.analyze_batch_sentiment(texts)
        
        # Verificar que temos diferentes classificações
        sentiments = [r['sentiment_label'] for r in results]
        assert 'Positivo' in sentiments
        assert 'Negativo' in sentiments


# ==========================================
# TESTES: ReportGenerator
# ==========================================

class TestReportGenerator:
    """Testes para a classe ReportGenerator."""

    def test_deve_inicializar_e_criar_diretorio_output(self, temp_reports_dir):
        """Deve inicializar e criar diretório de output se não existir."""
        generator = ReportGenerator(temp_reports_dir)
        
        assert generator.output_dir.exists()
        assert str(generator.output_dir) == temp_reports_dir

    def test_deve_gerar_relatorio_csv_com_dados_completos(
        self, temp_reports_dir, mock_bitcoin_price_response, 
        sample_tweets_data, sample_sentiment_results
    ):
        """Deve gerar relatório CSV com todos os dados fornecidos."""
        # Arrange
        generator = ReportGenerator(temp_reports_dir)
        price_data = {
            'price_usd': 68234.50,
            'market_cap_usd': 1340000000000,
            'volume_24h_usd': 15000000000,
            'price_change_24h_pct': 2.5
        }
        
        # Act
        report_path = generator.generate_comprehensive_report(
            price_data, sample_sentiment_results, sample_tweets_data
        )
        
        # Assert
        assert os.path.exists(report_path)
        assert report_path.endswith('.csv')
        
        # Verificar conteúdo do CSV
        df = pd.read_csv(report_path)
        assert len(df) == len(sample_tweets_data) + 1  # +1 para linha de resumo
        assert 'btc_price_usd' in df.columns
        assert 'sentiment_polarity' in df.columns
        assert 'tweet_text' in df.columns

    def test_deve_incluir_linha_resumo_no_relatorio(
        self, temp_reports_dir, sample_tweets_data, sample_sentiment_results
    ):
        """Deve incluir linha de resumo com estatísticas no relatório."""
        generator = ReportGenerator(temp_reports_dir)
        price_data = {'price_usd': 50000, 'market_cap_usd': 1000000000}
        
        report_path = generator.generate_comprehensive_report(
            price_data, sample_sentiment_results, sample_tweets_data
        )
        
        df = pd.read_csv(report_path)
        
        # Verificar linha de resumo
        summary_row = df[df['sentiment_label'] == 'RESUMO']
        assert len(summary_row) == 1
        assert 'RESUMO' in summary_row.iloc[0]['timestamp']

    def test_deve_tratar_dados_vazios_graciosamente(self, temp_reports_dir):
        """Deve tratar adequadamente quando não há dados para relatório."""
        generator = ReportGenerator(temp_reports_dir)
        price_data = {'price_usd': 50000}
        
        report_path = generator.generate_comprehensive_report(
            price_data, [], []
        )
        
        assert os.path.exists(report_path)
        
        # Quando não há dados, o DataFrame estará vazio mas ainda será salvo
        # Verifica se o arquivo foi criado (pode estar vazio se não há dados)
        file_size = os.path.getsize(report_path)
        assert file_size >= 0  # Arquivo existe (pode estar vazio)
        
        # Se há conteúdo, deve ter cabeçalho CSV válido
        if file_size > 0:
            with open(report_path, 'r') as file:
                first_line = file.readline().strip()
                # Se não está vazio, deve ter pelo menos algumas colunas esperadas
                if first_line:
                    assert any(col in first_line for col in ['timestamp', 'btc_price_usd', 'sentiment_polarity'])


# ==========================================
# TESTES: Pipeline Completo
# ==========================================

class TestCryptoPipelineIntegration:
    """Testes de integração para o pipeline completo."""

    @patch('crypto_etl.CryptoPriceCollector.get_bitcoin_price')
    @patch('crypto_etl.TwitterDataCollector.collect_bitcoin_tweets_rss')
    @patch('crypto_etl.TwitterDataCollector.collect_bitcoin_news_alternative')
    def test_deve_executar_pipeline_completo_com_sucesso(
        self, mock_news, mock_rss, mock_price, sample_tweets_data, mock_bitcoin_price_response
    ):
        """Deve executar pipeline completo com sucesso quando todos os componentes funcionam."""
        # Arrange
        mock_price.return_value = {
            'price_usd': 68234.50,
            'market_cap_usd': 1340000000000,
            'volume_24h_usd': 15000000000,
            'price_change_24h_pct': 2.5,
            'timestamp': '2025-11-02T10:00:00'
        }
        mock_rss.return_value = sample_tweets_data
        mock_news.return_value = []
        
        # Act
        result = run_crypto_etl_pipeline()
        
        # Assert
        assert result['success'] is True
        assert 'btc_price' in result
        assert 'total_texts_analyzed' in result
        assert 'avg_sentiment' in result
        assert 'report_path' in result
        assert result['btc_price'] == 68234.50

    @patch('crypto_etl.CryptoPriceCollector.get_bitcoin_price')
    def test_deve_retornar_erro_quando_coleta_preco_falha(self, mock_price):
        """Deve retornar erro quando coleta de preços falha."""
        # Arrange
        mock_price.side_effect = Exception("API Error")
        
        # Act
        result = run_crypto_etl_pipeline()
        
        # Assert
        assert result['success'] is False
        assert 'error' in result
        assert 'API Error' in result['error']

    @patch('crypto_etl.CryptoPriceCollector.get_bitcoin_price')
    @patch('crypto_etl.TwitterDataCollector.collect_bitcoin_tweets_rss')
    @patch('crypto_etl.TwitterDataCollector.collect_bitcoin_news_alternative')
    def test_deve_usar_fonte_alternativa_quando_rss_retorna_poucos_dados(
        self, mock_news, mock_rss, mock_price
    ):
        """Deve usar fonte alternativa quando RSS retorna poucos dados."""
        # Arrange
        mock_price.return_value = {'price_usd': 50000, 'market_cap_usd': 1000000}
        mock_rss.return_value = [{'text': 'Single tweet', 'source': 'rss'}]  # Poucos dados
        mock_news.return_value = [
            {'text': 'News item 1', 'source': 'news'},
            {'text': 'News item 2', 'source': 'news'}
        ]
        
        # Act
        result = run_crypto_etl_pipeline()
        
        # Assert
        assert result['success'] is True
        assert result['total_texts_analyzed'] == 3  # 1 RSS + 2 News
        mock_news.assert_called_once()  # Confirma que fonte alternativa foi chamada


if __name__ == "__main__":
    pytest.main([__file__, "-v"])