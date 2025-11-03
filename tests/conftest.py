"""
Configuração global de fixtures para testes do CryptoSentinel ETL.

Este arquivo centraliza fixtures comuns que podem ser reutilizadas
em múltiplos arquivos de teste.

Autor: Vitória
Data: 2025-11-02
"""

import pytest
import tempfile
from datetime import datetime
from unittest.mock import Mock


@pytest.fixture(scope="session")
def sample_bitcoin_price_data():
    """Fixture de sessão com dados de exemplo do preço Bitcoin."""
    return {
        'price_usd': 68234.50,
        'market_cap_usd': 1340000000000.0,
        'volume_24h_usd': 15000000000.0,
        'price_change_24h_pct': 2.5,
        'timestamp': '2025-11-02T10:00:00.000000'
    }


@pytest.fixture(scope="session") 
def sample_crypto_news_data():
    """Fixture de sessão com dados de exemplo de notícias crypto."""
    return [
        {
            'text': 'Bitcoin reaches all-time high with strong institutional adoption',
            'description': 'Major companies continue to add Bitcoin to their treasury reserves',
            'source': 'crypto_news',
            'published_date': '2025-11-02T09:00:00',
            'collected_at': '2025-11-02T09:05:00'
        },
        {
            'text': 'Ethereum 2.0 staking rewards attract more validators',
            'description': 'The network sees increased participation in proof-of-stake',
            'source': 'crypto_news', 
            'published_date': '2025-11-02T08:30:00',
            'collected_at': '2025-11-02T08:35:00'
        },
        {
            'text': 'Regulatory clarity boosts cryptocurrency market sentiment',
            'description': 'New guidelines provide framework for digital asset operations',
            'source': 'crypto_news',
            'published_date': '2025-11-02T08:00:00', 
            'collected_at': '2025-11-02T08:05:00'
        }
    ]


@pytest.fixture
def temp_directory():
    """Fixture que fornece diretório temporário limpo para cada teste."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def mock_successful_http_response():
    """Fixture com mock de resposta HTTP bem-sucedida."""
    response = Mock()
    response.status_code = 200
    response.raise_for_status.return_value = None
    return response


@pytest.fixture
def mock_failed_http_response():
    """Fixture com mock de resposta HTTP com erro."""
    response = Mock()
    response.status_code = 500
    response.raise_for_status.side_effect = Exception("HTTP 500 Error")
    return response


@pytest.fixture(scope="function")
def current_timestamp():
    """Fixture que fornece timestamp atual formatado."""
    return datetime.now().isoformat()


@pytest.fixture
def valid_rss_xml():
    """Fixture com XML RSS válido para testes."""
    return """<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0">
        <channel>
            <title>Bitcoin News Feed</title>
            <description>Latest Bitcoin and cryptocurrency news</description>
            <item>
                <title>Bitcoin breaks resistance level at $70,000</title>
                <description>Technical analysis shows bullish momentum continues</description>
                <pubDate>Mon, 02 Nov 2025 10:00:00 GMT</pubDate>
            </item>
            <item>
                <title>Crypto adoption grows among institutional investors</title>
                <description>Survey reveals increased Bitcoin allocation in portfolios</description>
                <pubDate>Mon, 02 Nov 2025 09:30:00 GMT</pubDate>
            </item>
            <item>
                <title>Apple announces quarterly earnings</title>
                <description>No cryptocurrency content in this item</description>
                <pubDate>Mon, 02 Nov 2025 09:00:00 GMT</pubDate>
            </item>
        </channel>
    </rss>"""


@pytest.fixture
def sentiment_test_cases():
    """Fixture com casos de teste para análise de sentimento."""
    return [
        {
            'text': 'Bitcoin is absolutely amazing! Best investment ever!',
            'expected_label': 'Positivo',
            'expected_polarity_sign': 1  # Positivo
        },
        {
            'text': 'Bitcoin crash ruined my portfolio completely',
            'expected_label': 'Negativo', 
            'expected_polarity_sign': -1  # Negativo
        },
        {
            'text': 'Bitcoin price today is normal',
            'expected_label': 'Neutro',
            'expected_polarity_sign': 0  # Neutro (próximo de zero)
        },
        {
            'text': '',  # Texto vazio
            'expected_label': 'Neutro',
            'expected_polarity_sign': 0
        }
    ]


# Configurações globais do pytest
def pytest_configure(config):
    """Configuração global do pytest."""
    # Adicionar marcadores customizados
    config.addinivalue_line(
        "markers", "integration: marca testes de integração"
    )
    config.addinivalue_line(
        "markers", "unit: marca testes unitários"
    )
    config.addinivalue_line(
        "markers", "slow: marca testes que demoram para executar"
    )


def pytest_collection_modifyitems(config, items):
    """Modifica itens da coleção de testes."""
    # Adicionar marcador 'unit' para todos os testes por padrão
    for item in items:
        if "integration" not in item.keywords:
            item.add_marker(pytest.mark.unit)