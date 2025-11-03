"""
CryptoSentinel ETL - Módulo principal para extração, transformação e carregamento de dados
sobre Bitcoin e análise de sentimento de tweets.

Este módulo implementa as funções core do pipeline ETL:
- Coleta de preços do Bitcoin via CoinGecko API
- Extração de tweets públicos sobre Bitcoin
- Análise de sentimento textual com TextBlob
- Geração de relatórios estruturados

Autor: Vitória
Data: 2025-11-02
"""

import logging
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from textblob import TextBlob
from pathlib import Path
import json
import time
from sentiment_utils import CryptoSentimentAnalyzer, create_enhanced_sentiment_analyzer

# Importar S3 utils se disponível
try:
    import sys
    sys.path.append('.')
    from s3_utils import S3Manager
    S3_AVAILABLE = True
except ImportError:
    S3_AVAILABLE = False
    print("⚠️ S3Utils não disponível - upload para S3 desabilitado")

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CryptoPriceCollector:
    """
    Classe responsável pela coleta de dados de preços de criptomoedas
    via APIs públicas como CoinGecko.
    """
    
    def __init__(self, base_url: str = "https://api.coingecko.com/api/v3"):
        """
        Inicializa o coletor de preços.
        
        Args:
            base_url: URL base da API CoinGecko
        """
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'CryptoSentinel-ETL/1.0'
        })
    
    def get_bitcoin_price(self) -> Dict[str, float]:
        """
        Coleta o preço atual do Bitcoin em USD via CoinGecko API.
        
        Returns:
            Dict contendo informações de preço do Bitcoin
            
        Raises:
            Exception: Se houver erro na requisição da API
        """
        endpoint = f"{self.base_url}/simple/price"
        params = {
            'ids': 'bitcoin',
            'vs_currencies': 'usd',
            'include_24hr_change': 'true',
            'include_market_cap': 'true',
            'include_24hr_vol': 'true'
        }
        
        try:
            logger.info("🔍 Coletando preço atual do Bitcoin...")
            response = self.session.get(endpoint, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            bitcoin_data = data.get('bitcoin', {})
            
            price_info = {
                'price_usd': bitcoin_data.get('usd', 0),
                'market_cap_usd': bitcoin_data.get('usd_market_cap', 0),
                'volume_24h_usd': bitcoin_data.get('usd_24h_vol', 0),
                'price_change_24h_pct': bitcoin_data.get('usd_24h_change', 0),
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"💰 Preço Bitcoin: ${price_info['price_usd']:,.2f} USD")
            logger.info(f"📈 Variação 24h: {price_info['price_change_24h_pct']:.2f}%")
            
            return price_info
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erro ao coletar preço do Bitcoin: {e}")
            raise Exception(f"Falha na coleta de preços: {e}")
        except Exception as e:
            logger.error(f"❌ Erro inesperado: {e}")
            raise


class TwitterDataCollector:
    """
    Classe responsável pela coleta de dados do Twitter/X sobre Bitcoin
    usando fontes públicas e APIs abertas.
    """
    
    def __init__(self):
        """Inicializa o coletor de dados do Twitter."""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Linux) CryptoSentinel-ETL/1.0'
        })
    
    def collect_bitcoin_tweets_rss(self, max_tweets: int = 20) -> List[Dict[str, str]]:
        """
        Coleta tweets sobre Bitcoin via RSS feeds públicos.
        
        Args:
            max_tweets: Número máximo de tweets para coletar
            
        Returns:
            Lista de dicionários contendo dados dos tweets
        """
        # URLs de RSS feeds públicos sobre Bitcoin
        rss_sources = [
            "https://rss.cnn.com/rss/money_latest.rss",  # CNN Money
            "https://feeds.finance.yahoo.com/rss/2.0/headline?s=BTC-USD&region=US&lang=en-US"  # Yahoo Finance
        ]
        
        tweets_data = []
        
        for rss_url in rss_sources:
            try:
                logger.info(f"📡 Coletando dados de: {rss_url}")
                response = self.session.get(rss_url, timeout=15)
                response.raise_for_status()
                
                # Parse do XML RSS
                root = ET.fromstring(response.content)
                
                # Buscar itens que mencionam Bitcoin
                items = root.findall('.//item')
                
                for item in items[:max_tweets//2]:  # Dividir entre as fontes
                    title_elem = item.find('title')
                    desc_elem = item.find('description')
                    pub_date_elem = item.find('pubDate')
                    
                    if title_elem is not None:
                        title = title_elem.text or ""
                        description = desc_elem.text if desc_elem is not None else ""
                        pub_date = pub_date_elem.text if pub_date_elem is not None else ""
                        
                        # Filtrar apenas conteúdo relacionado ao Bitcoin
                        content = f"{title} {description}".lower()
                        if any(term in content for term in ['bitcoin', 'btc', 'crypto']):
                            tweets_data.append({
                                'text': title,
                                'description': description,
                                'source': 'rss_feed',
                                'published_date': pub_date,
                                'collected_at': datetime.now().isoformat()
                            })
                
                time.sleep(1)  # Rate limiting respeitoso
                
            except Exception as e:
                logger.warning(f"⚠️ Erro ao coletar de {rss_url}: {e}")
                continue
        
        logger.info(f"✅ {len(tweets_data)} posts coletados via RSS")
        return tweets_data
    
    def collect_bitcoin_news_alternative(self) -> List[Dict[str, str]]:
        """
        Método alternativo para coletar notícias sobre Bitcoin
        usando APIs de notícias públicas.
        
        Returns:
            Lista de notícias sobre Bitcoin
        """
        try:
            # CryptoCompare News API (público)
            url = "https://min-api.cryptocompare.com/data/v2/news/?lang=EN&sortOrder=latest"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            news_items = []
            
            if 'Data' in data:
                for item in data['Data'][:15]:  # Limitar a 15 notícias
                    news_items.append({
                        'text': item.get('title', ''),
                        'description': item.get('body', '')[:400],  # Primeiros 400 chars
                        'source': 'crypto_news',
                        'published_date': datetime.fromtimestamp(
                            item.get('published_on', 0)
                        ).isoformat(),
                        'collected_at': datetime.now().isoformat()
                    })
            
            logger.info(f"✅ {len(news_items)} notícias coletadas")
            return news_items
            
        except Exception as e:
            logger.warning(f"⚠️ Erro ao coletar notícias alternativas: {e}")
            return []


class SentimentAnalyzer:
    """
    Classe responsável pela análise de sentimento de textos
    relacionados ao Bitcoin usando análise aprimorada para crypto.
    """
    
    def __init__(self):
        """Inicializa o analisador de sentimento aprimorado."""
        self.enhanced_analyzer = create_enhanced_sentiment_analyzer()
        # Manter compatibilidade com thresholds originais
        self.positive_threshold = 0.1
        self.negative_threshold = -0.1
    
    def analyze_text_sentiment(self, text: str) -> Dict[str, any]:
        """
        Analisa o sentimento de um texto individual usando método aprimorado.
        
        Args:
            text: Texto para análise
            
        Returns:
            Dict com métricas de sentimento (compatível com versão anterior)
        """
        if not text or not isinstance(text, str):
            return {
                'polarity': 0.0,
                'subjectivity': 0.0,
                'sentiment_label': 'Neutro',
                'confidence': 'Baixa'
            }
        
        try:
            # Usar análise aprimorada
            enhanced_result = self.enhanced_analyzer.analyze_enhanced_sentiment(text)
            
            # Retornar no formato esperado pelo resto do sistema
            return {
                'polarity': enhanced_result['polarity'],
                'subjectivity': enhanced_result['subjectivity'],
                'sentiment_label': enhanced_result['sentiment_label'],
                'confidence': enhanced_result['confidence'],
                # Campos adicionais para debugging/logging
                'analysis_method': enhanced_result.get('analysis_method', 'enhanced'),
                'crypto_matches': enhanced_result.get('crypto_matches', 0),
                'crypto_terms_found': enhanced_result.get('crypto_terms_found', [])
            }
            
        except Exception as e:
            logger.error(f"❌ Erro na análise de sentimento aprimorada: {e}")
            # Fallback para análise básica
            return self._fallback_analysis(text)
    
    def _fallback_analysis(self, text: str) -> Dict[str, any]:
        """Análise de fallback usando TextBlob básico."""
        try:
            blob = TextBlob(text)
            polarity = blob.sentiment.polarity
            subjectivity = blob.sentiment.subjectivity
            
            if polarity > self.positive_threshold:
                sentiment_label = 'Positivo'
                confidence = 'Alta' if polarity > 0.3 else 'Média'
            elif polarity < self.negative_threshold:
                sentiment_label = 'Negativo'
                confidence = 'Alta' if polarity < -0.3 else 'Média'
            else:
                sentiment_label = 'Neutro'
                confidence = 'Média'
            
            return {
                'polarity': round(polarity, 4),
                'subjectivity': round(subjectivity, 4),
                'sentiment_label': sentiment_label,
                'confidence': confidence,
                'analysis_method': 'textblob_fallback',
                'crypto_matches': 0,
                'crypto_terms_found': []
            }
        except Exception:
            return {
                'polarity': 0.0,
                'subjectivity': 0.0,
                'sentiment_label': 'Erro',
                'confidence': 'Baixa',
                'analysis_method': 'error',
                'crypto_matches': 0,
                'crypto_terms_found': []
            }
    
    def analyze_batch_sentiment(self, texts: List[str]) -> List[Dict[str, any]]:
        """
        Analisa o sentimento de uma lista de textos usando método aprimorado.
        
        Args:
            texts: Lista de textos para análise
            
        Returns:
            Lista com análises de sentimento
        """
        if not texts:
            return []
        
        logger.info(f"🧠 Analisando sentimento de {len(texts)} textos com método aprimorado...")
        
        # Usar o analisador aprimorado diretamente
        enhanced_results = self.enhanced_analyzer.analyze_batch_sentiment(texts)
        
        # Converter para formato compatível
        results = []
        for enhanced_result in enhanced_results:
            compatible_result = {
                'polarity': enhanced_result['polarity'],
                'subjectivity': enhanced_result['subjectivity'],
                'sentiment_label': enhanced_result['sentiment_label'],
                'confidence': enhanced_result['confidence'],
                'text_index': enhanced_result.get('text_index', 0),
                'original_text': enhanced_result.get('original_text', ''),
                # Campos extras para análise avançada
                'analysis_method': enhanced_result.get('analysis_method', 'enhanced'),
                'crypto_matches': enhanced_result.get('crypto_matches', 0),
                'crypto_terms_found': enhanced_result.get('crypto_terms_found', []),
                'textblob_polarity': enhanced_result.get('textblob_polarity', 0.0),
                'crypto_score': enhanced_result.get('crypto_score', 0.0),
                'emoji_score': enhanced_result.get('emoji_score', 0.0)
            }
            results.append(compatible_result)
        
        # Log estatísticas adicionais
        enhanced_count = len([r for r in results if r['analysis_method'] == 'enhanced_crypto'])
        total_crypto_matches = sum(r['crypto_matches'] for r in results)
        
        if enhanced_count > 0:
            logger.info(f"� Análises crypto aprimoradas: {enhanced_count}/{len(results)} ({enhanced_count/len(results)*100:.1f}%)")
            logger.info(f"� Total de termos crypto detectados: {total_crypto_matches}")
        
        return results


class ReportGenerator:
    """
    Classe responsável pela geração de relatórios estruturados
    combinando dados de preço e sentimento.
    """
    
    def __init__(self, output_dir: str = "./data/reports"):
        """
        Inicializa o gerador de relatórios.
        
        Args:
            output_dir: Diretório para salvar relatórios
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_comprehensive_report(
        self, 
        price_data: Dict[str, float],
        sentiment_results: List[Dict[str, any]],
        tweets_data: List[Dict[str, str]]
    ) -> str:
        """
        Gera relatório abrangente combinando preço e sentimento.
        
        Args:
            price_data: Dados de preço do Bitcoin
            sentiment_results: Resultados da análise de sentimento
            tweets_data: Dados originais dos tweets/posts
            
        Returns:
            Caminho do arquivo de relatório gerado
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"crypto_sentinel_report_{timestamp}.csv"
        filepath = self.output_dir / filename
        
        # Combinar dados para DataFrame
        report_data = []
        
        for i, (sentiment, tweet) in enumerate(zip(sentiment_results, tweets_data)):
            row = {
                'timestamp': datetime.now().isoformat(),
                'btc_price_usd': price_data.get('price_usd', 0),
                'btc_market_cap': price_data.get('market_cap_usd', 0),
                'btc_volume_24h': price_data.get('volume_24h_usd', 0),
                'btc_change_24h_pct': price_data.get('price_change_24h_pct', 0),
                'tweet_index': i,
                'tweet_text': tweet.get('text', '')[:500],
                'tweet_source': tweet.get('source', 'unknown'),
                'sentiment_polarity': sentiment.get('polarity', 0),
                'sentiment_subjectivity': sentiment.get('subjectivity', 0),
                'sentiment_label': sentiment.get('sentiment_label', 'Neutro'),
                'sentiment_confidence': sentiment.get('confidence', 'Baixa'),
                # Campos avançados da análise aprimorada
                'analysis_method': sentiment.get('analysis_method', 'standard'),
                'crypto_matches': sentiment.get('crypto_matches', 0),
                'crypto_terms_found': ', '.join(sentiment.get('crypto_terms_found', [])),
                'crypto_score': sentiment.get('crypto_score', 0),
                'emoji_score': sentiment.get('emoji_score', 0)
            }
            report_data.append(row)
        
        # Criar DataFrame e salvar
        df = pd.DataFrame(report_data)
        
        # Adicionar estatísticas resumo
        if not df.empty:
            avg_sentiment = df['sentiment_polarity'].mean()
            positive_ratio = len(df[df['sentiment_polarity'] > 0.1]) / len(df)
            negative_ratio = len(df[df['sentiment_polarity'] < -0.1]) / len(df)
            
            # Adicionar linha de resumo
            summary_row = {
                'timestamp': f"RESUMO_{timestamp}",
                'btc_price_usd': price_data.get('price_usd', 0),
                'sentiment_polarity': avg_sentiment,
                'tweet_text': f"Análise de {len(df)} posts | Pos: {positive_ratio:.1%} | Neg: {negative_ratio:.1%}",
                'sentiment_label': 'RESUMO'
            }
            
            df = pd.concat([df, pd.DataFrame([summary_row])], ignore_index=True)
        
        # Salvar relatório
        df.to_csv(filepath, index=False, encoding='utf-8')
        
        logger.info(f"📄 Relatório salvo: {filepath}")
        logger.info(f"📊 Total de registros: {len(df)-1}")  # -1 para excluir linha de resumo
        
        return str(filepath)


# Função principal para execução local
def run_crypto_etl_pipeline(upload_to_s3=True) -> Dict[str, any]:
    """
    Executa o pipeline completo de ETL do CryptoSentinel localmente.
    
    Args:
        upload_to_s3: Se True, faz upload do relatório para S3
    
    Returns:
        Dict com resultados da execução
    """
    logger.info("🚀 Iniciando pipeline CryptoSentinel ETL...")
    
    try:
        # 1. Coletar preço do Bitcoin
        price_collector = CryptoPriceCollector()
        price_data = price_collector.get_bitcoin_price()
        
        # 2. Coletar posts/tweets sobre Bitcoin
        twitter_collector = TwitterDataCollector()
        tweets_data = twitter_collector.collect_bitcoin_tweets_rss()
        
        # Fallback para fonte alternativa se necessário
        if len(tweets_data) < 5:
            logger.info("🔄 Coletando dados de fonte alternativa...")
            alt_data = twitter_collector.collect_bitcoin_news_alternative()
            tweets_data.extend(alt_data)
        
        if not tweets_data:
            raise Exception("Nenhum dado de texto coletado")
        
        # 3. Analisar sentimento
        sentiment_analyzer = SentimentAnalyzer()
        texts_to_analyze = [tweet.get('text', '') for tweet in tweets_data]
        sentiment_results = sentiment_analyzer.analyze_batch_sentiment(texts_to_analyze)
        
        # 4. Gerar relatório
        report_generator = ReportGenerator()
        report_path = report_generator.generate_comprehensive_report(
            price_data, sentiment_results, tweets_data
        )
        
        # 5. Compilar resultados
        execution_results = {
            'success': True,
            'timestamp': datetime.now().isoformat(),
            'btc_price': price_data.get('price_usd', 0),
            'total_texts_analyzed': len(tweets_data),
            'avg_sentiment': sum(r.get('polarity', 0) for r in sentiment_results) / len(sentiment_results),
            'report_path': report_path,
            'execution_time_seconds': None  # Pode ser calculado se necessário
        }
        
        # 6. Upload para S3 (se habilitado)
        if upload_to_s3 and S3_AVAILABLE:
            try:
                logger.info("☁️ Fazendo upload para S3...")
                s3_manager = S3Manager()
                upload_result = s3_manager.upload_report(report_path)
                
                if upload_result.get('success'):
                    execution_results['s3_upload'] = upload_result
                    logger.info(f"✅ Upload S3 concluído: {upload_result['s3_url']}")
                else:
                    logger.warning(f"⚠️ Falha no upload S3: {upload_result.get('error')}")
                    execution_results['s3_upload'] = upload_result
                    
            except Exception as e:
                logger.warning(f"⚠️ Erro no upload S3: {e}")
                execution_results['s3_upload'] = {'success': False, 'error': str(e)}
        elif upload_to_s3:
            logger.warning("⚠️ Upload S3 solicitado mas S3Utils não disponível")
        
        logger.info("✅ Pipeline CryptoSentinel executado com sucesso!")
        logger.info(f"💰 Preço BTC: ${execution_results['btc_price']:,.2f}")
        logger.info(f"🧠 Sentimento médio: {execution_results['avg_sentiment']:.3f}")
        logger.info(f"📊 Textos analisados: {execution_results['total_texts_analyzed']}")
        
        return execution_results
        
    except Exception as e:
        logger.error(f"❌ Erro na execução do pipeline: {e}")
        return {
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }


if __name__ == "__main__":
    # Execução local para testes
    results = run_crypto_etl_pipeline()
    print(f"\n🎯 Resultados da execução:")
    print(json.dumps(results, indent=2, ensure_ascii=False))