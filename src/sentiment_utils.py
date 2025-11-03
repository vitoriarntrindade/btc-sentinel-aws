"""
Utilitários avançados para análise de sentimento em dados crypto.

Este módulo contém implementações otimizadas e especializadas para análise
de sentimento em textos relacionados a criptomoedas, melhorando significativamente
a precisão em relação ao TextBlob básico.

Autor: Vitória
Data: 2025-11-02
"""

import re
import logging
from typing import Dict, List, Tuple, Optional
from textblob import TextBlob
from datetime import datetime

logger = logging.getLogger(__name__)


class CryptoSentimentAnalyzer:
    """
    Analisador de sentimento especializado para conteúdo crypto.
    
    Combina TextBlob com dicionários customizados de termos crypto,
    melhorando significativamente a precisão para este domínio específico.
    """
    
    def __init__(self):
        """Inicializa o analisador com dicionários especializados."""
        self._init_crypto_dictionaries()
        self._init_preprocessing_patterns()
        
    def _init_crypto_dictionaries(self):
        """Inicializa dicionários de termos crypto com scores de sentimento."""
        
        # Termos altamente positivos (bullish)
        self.crypto_positive_high = {
            'moon': 0.9, 'mooning': 0.9, 'lambo': 0.9, 'rocket': 0.8,
            'ath': 0.8, 'all-time high': 0.8, 'diamond hands': 0.7,
            'hodl': 0.6, 'hold': 0.4, 'pump': 0.6, 'surge': 0.7,
            'rally': 0.7, 'bull run': 0.8, 'bullish': 0.7, 'breakout': 0.7,
            'green candle': 0.6, 'green': 0.3, 'profit': 0.6, 'gains': 0.7,
            'buy the dip': 0.5, 'accumulate': 0.4, 'strong hands': 0.6
        }
        
        # Termos moderadamente positivos
        self.crypto_positive_moderate = {
            'adoption': 0.4, 'institutional': 0.3, 'mainstream': 0.4,
            'partnership': 0.5, 'integration': 0.4, 'upgrade': 0.5,
            'bullish signal': 0.6, 'golden cross': 0.6, 'support level': 0.3,
            'resistance break': 0.5, 'volume spike': 0.4, 'whale accumulation': 0.5
        }
        
        # Termos altamente negativos (bearish)
        self.crypto_negative_high = {
            'rekt': -0.9, 'liquidation': -0.9, 'liquidated': -0.9,
            'rug pull': -0.9, 'rugpull': -0.9, 'scam': -0.9, 'ponzi': -0.9,
            'crash': -0.8, 'dump': -0.7, 'dumping': -0.7, 'bear market': -0.7,
            'bearish': -0.7, 'panic sell': -0.8, 'panic selling': -0.8,
            'death cross': -0.7, 'red candle': -0.6, 'red': -0.3,
            'loss': -0.6, 'losses': -0.6, 'bleeding': -0.7, 'brutal': -0.8
        }
        
        # Termos moderadamente negativos
        self.crypto_negative_moderate = {
            'correction': -0.3, 'dip': -0.2, 'pullback': -0.2, 'decline': -0.4,
            'sell': -0.3, 'selling pressure': -0.5, 'resistance': -0.2,
            'overhead resistance': -0.3, 'weak hands': -0.4, 'fud': -0.6,
            'fear': -0.5, 'uncertainty': -0.3, 'doubt': -0.4, 'volatile': -0.2,
            'manipulation': -0.6, 'whale dump': -0.7, 'paper hands': -0.5
        }
        
        # Termos neutros/informativos
        self.crypto_neutral = {
            'blockchain': 0.0, 'mining': 0.0, 'hash': 0.0, 'wallet': 0.0,
            'exchange': 0.0, 'transaction': 0.0, 'block': 0.0, 'node': 0.0,
            'protocol': 0.0, 'fork': 0.0, 'halving': 0.0, 'difficulty': 0.0,
            'market cap': 0.0, 'volume': 0.0, 'liquidity': 0.0, 'trading': 0.0,
            'analysis': 0.0, 'chart': 0.0, 'technical': 0.0, 'fundamental': 0.0
        }
        
        # Combinar todos os dicionários
        self.crypto_terms = {}
        self.crypto_terms.update(self.crypto_positive_high)
        self.crypto_terms.update(self.crypto_positive_moderate)
        self.crypto_terms.update(self.crypto_negative_high)
        self.crypto_terms.update(self.crypto_negative_moderate)
        self.crypto_terms.update(self.crypto_neutral)
        
    def _init_preprocessing_patterns(self):
        """Inicializa padrões para preprocessamento de texto."""
        # Padrões de emoji para sentimento
        self.positive_emojis = ['🚀', '🌙', '💎', '🔥', '📈', '💚', '✅', '🎉', '💪', '🔝']
        self.negative_emojis = ['📉', '💔', '😭', '😰', '🔴', '❌', '💸', '⬇️', '😱', '🩸']
        
        # Padrões regex para limpeza
        self.url_pattern = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
        self.mention_pattern = re.compile(r'@\w+')
        self.hashtag_pattern = re.compile(r'#\w+')
        
    def preprocess_text(self, text: str) -> Tuple[str, float]:
        """
        Preprocessa o texto e extrai signals de emojis.
        
        Args:
            text: Texto para preprocessar
            
        Returns:
            Tuple com texto limpo e score de emoji
        """
        if not text or not isinstance(text, str):
            return "", 0.0
        
        # Score de emojis
        emoji_score = 0.0
        for emoji in self.positive_emojis:
            emoji_score += text.count(emoji) * 0.3
        for emoji in self.negative_emojis:
            emoji_score -= text.count(emoji) * 0.3
        
        # Limpar texto
        cleaned = text.lower()
        cleaned = self.url_pattern.sub('', cleaned)
        cleaned = self.mention_pattern.sub('', cleaned)
        cleaned = re.sub(r'\s+', ' ', cleaned)  # Múltiplos espaços
        cleaned = cleaned.strip()
        
        return cleaned, emoji_score
    
    def analyze_crypto_terms(self, text: str) -> Dict[str, float]:
        """
        Analisa termos crypto específicos no texto.
        
        Args:
            text: Texto para analisar
            
        Returns:
            Dict com análise de termos crypto
        """
        text_lower = text.lower()
        
        crypto_score = 0.0
        matches = []
        
        # Buscar termos crypto (ordenar por tamanho para pegar frases primeiro)
        sorted_terms = sorted(self.crypto_terms.items(), key=lambda x: len(x[0]), reverse=True)
        
        for term, score in sorted_terms:
            if term in text_lower:
                crypto_score += score
                matches.append((term, score))
                # Remover termo encontrado para evitar dupla contagem
                text_lower = text_lower.replace(term, ' ')
        
        return {
            'crypto_score': crypto_score,
            'matches': matches,
            'match_count': len(matches)
        }
    
    def calculate_confidence(self, crypto_matches: int, textblob_polarity: float, emoji_score: float) -> str:
        """
        Calcula nível de confiança da análise.
        
        Args:
            crypto_matches: Número de termos crypto encontrados
            textblob_polarity: Score do TextBlob
            emoji_score: Score dos emojis
            
        Returns:
            Nível de confiança (Alta, Média, Baixa)
        """
        confidence_score = 0
        
        # Confiança baseada em matches crypto
        if crypto_matches >= 3:
            confidence_score += 3
        elif crypto_matches >= 2:
            confidence_score += 2
        elif crypto_matches >= 1:
            confidence_score += 1
        
        # Confiança baseada em TextBlob
        if abs(textblob_polarity) > 0.5:
            confidence_score += 2
        elif abs(textblob_polarity) > 0.2:
            confidence_score += 1
        
        # Confiança baseada em emojis
        if abs(emoji_score) > 0.3:
            confidence_score += 1
        
        if confidence_score >= 4:
            return 'Alta'
        elif confidence_score >= 2:
            return 'Média'
        else:
            return 'Baixa'
    
    def analyze_enhanced_sentiment(self, text: str) -> Dict[str, any]:
        """
        Análise de sentimento aprimorada combinando múltiplas abordagens.
        
        Args:
            text: Texto para analisar
            
        Returns:
            Dict com análise completa de sentimento
        """
        if not text or not isinstance(text, str):
            return self._get_neutral_result()
        
        try:
            # 1. Preprocessamento
            cleaned_text, emoji_score = self.preprocess_text(text)
            
            # 2. Análise TextBlob base
            blob = TextBlob(cleaned_text)
            textblob_polarity = blob.sentiment.polarity
            subjectivity = blob.sentiment.subjectivity
            
            # 3. Análise crypto específica
            crypto_analysis = self.analyze_crypto_terms(cleaned_text)
            
            # 4. Combinação de scores (pesos otimizados)
            if crypto_analysis['match_count'] > 0:
                # Se há termos crypto, dar mais peso ao score crypto
                weight_crypto = 0.6
                weight_textblob = 0.3
                weight_emoji = 0.1
            else:
                # Se não há termos crypto, confiar mais no TextBlob
                weight_crypto = 0.0
                weight_textblob = 0.8
                weight_emoji = 0.2
            
            final_polarity = (
                crypto_analysis['crypto_score'] * weight_crypto +
                textblob_polarity * weight_textblob +
                emoji_score * weight_emoji
            )
            
            # 5. Classificação final
            if final_polarity > 0.1:
                sentiment_label = 'Positivo'
            elif final_polarity < -0.1:
                sentiment_label = 'Negativo'
            else:
                sentiment_label = 'Neutro'
            
            # 6. Calcular confiança
            confidence = self.calculate_confidence(
                crypto_analysis['match_count'],
                textblob_polarity,
                emoji_score
            )
            
            return {
                'polarity': round(final_polarity, 4),
                'subjectivity': round(subjectivity, 4),
                'sentiment_label': sentiment_label,
                'confidence': confidence,
                'textblob_polarity': round(textblob_polarity, 4),
                'crypto_score': round(crypto_analysis['crypto_score'], 4),
                'emoji_score': round(emoji_score, 4),
                'crypto_matches': crypto_analysis['match_count'],
                'crypto_terms_found': [match[0] for match in crypto_analysis['matches']],
                'analysis_method': 'enhanced_crypto' if crypto_analysis['match_count'] > 0 else 'textblob_standard'
            }
            
        except Exception as e:
            logger.error(f"❌ Erro na análise de sentimento: {e}")
            return self._get_neutral_result()
    
    def _get_neutral_result(self) -> Dict[str, any]:
        """Retorna resultado neutro para casos de erro."""
        return {
            'polarity': 0.0,
            'subjectivity': 0.0,
            'sentiment_label': 'Neutro',
            'confidence': 'Baixa',
            'textblob_polarity': 0.0,
            'crypto_score': 0.0,
            'emoji_score': 0.0,
            'crypto_matches': 0,
            'crypto_terms_found': [],
            'analysis_method': 'error_fallback'
        }
    
    def analyze_batch_sentiment(self, texts: List[str]) -> List[Dict[str, any]]:
        """
        Analisa sentimento de uma lista de textos.
        
        Args:
            texts: Lista de textos para analisar
            
        Returns:
            Lista com análises de sentimento
        """
        if not texts:
            return []
        
        logger.info(f"🧠 Analisando sentimento de {len(texts)} textos com método aprimorado...")
        
        results = []
        
        for i, text in enumerate(texts):
            analysis = self.analyze_enhanced_sentiment(text)
            analysis['text_index'] = i
            analysis['original_text'] = text[:100] + "..." if len(text) > 100 else text
            results.append(analysis)
        
        # Estatísticas do batch
        polarities = [r['polarity'] for r in results if r['polarity'] != 0]
        crypto_enhanced = len([r for r in results if r['analysis_method'] == 'enhanced_crypto'])
        
        if polarities:
            avg_polarity = sum(polarities) / len(polarities)
            positive_count = len([p for p in polarities if p > 0.1])
            negative_count = len([p for p in polarities if p < -0.1])
            neutral_count = len(polarities) - positive_count - negative_count
            
            logger.info(f"📊 Sentimento médio: {avg_polarity:.3f}")
            logger.info(f"📈 Positivos: {positive_count}, Negativos: {negative_count}, Neutros: {neutral_count}")
            logger.info(f"🚀 Análises crypto aprimoradas: {crypto_enhanced}/{len(texts)} ({crypto_enhanced/len(texts)*100:.1f}%)")
        
        return results


class TextPreprocessor:
    """
    Utilitário para preprocessamento avançado de texto.
    """
    
    @staticmethod
    def clean_financial_text(text: str) -> str:
        """
        Limpa texto financeiro mantendo informações relevantes.
        
        Args:
            text: Texto para limpar
            
        Returns:
            Texto limpo
        """
        if not text:
            return ""
        
        # Preservar símbolos monetários e percentuais
        cleaned = text
        
        # Remover URLs
        cleaned = re.sub(r'http[s]?://\S+', '', cleaned)
        
        # Remover mentions mas preservar $SYMBOLS
        cleaned = re.sub(r'@\w+', '', cleaned)
        
        # Normalizar espaços
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        return cleaned.strip()
    
    @staticmethod
    def extract_price_mentions(text: str) -> List[float]:
        """
        Extrai menções de preços do texto.
        
        Args:
            text: Texto para analisar
            
        Returns:
            Lista de preços encontrados
        """
        # Padrões de preço: $1234, $1,234.56, etc.
        price_patterns = [
            r'\$[\d,]+\.?\d*',
            r'USD?\s*[\d,]+\.?\d*',
            r'[\d,]+\.?\d*\s*dollars?'
        ]
        
        prices = []
        for pattern in price_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                # Extrair apenas números
                price_str = re.sub(r'[^\d.]', '', match)
                try:
                    price = float(price_str)
                    if price > 0:
                        prices.append(price)
                except ValueError:
                    continue
        
        return prices


def create_enhanced_sentiment_analyzer() -> CryptoSentimentAnalyzer:
    """
    Factory function para criar analisador de sentimento aprimorado.
    
    Returns:
        Instância configurada do CryptoSentimentAnalyzer
    """
    return CryptoSentimentAnalyzer()


# Função de conveniência para análise rápida
def quick_sentiment_analysis(text: str) -> Dict[str, any]:
    """
    Análise rápida de sentimento para um texto.
    
    Args:
        text: Texto para analisar
        
    Returns:
        Resultado da análise
    """
    analyzer = create_enhanced_sentiment_analyzer()
    return analyzer.analyze_enhanced_sentiment(text)


if __name__ == "__main__":
    # Teste rápido do módulo
    analyzer = create_enhanced_sentiment_analyzer()
    
    test_texts = [
        "Bitcoin to the moon! 🚀 Diamond hands HODL",
        "Bitcoin crash is devastating, total rekt",
        "Bitcoin technical analysis shows consolidation"
    ]
    
    print("🧪 Teste do Analisador Aprimorado:")
    print("=" * 50)
    
    for text in test_texts:
        result = analyzer.analyze_enhanced_sentiment(text)
        print(f"\n📝 '{text}'")
        print(f"📊 {result['sentiment_label']} ({result['polarity']:+.3f})")
        print(f"🎯 Confiança: {result['confidence']}")
        print(f"🔧 Método: {result['analysis_method']}")
        if result['crypto_terms_found']:
            print(f"💎 Termos crypto: {', '.join(result['crypto_terms_found'])}")