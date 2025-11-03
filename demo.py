#!/usr/bin/env python3
"""
Script de demonstração do CryptoSentinel ETL.

Este script executa o pipeline ETL completo e apresenta os resultados
de forma amigável, simulando o que seria a execução em produção.

Uso:
    python demo.py

Autor: Vitória
Data: 2025-11-02
"""

import sys
from pathlib import Path
import json
from datetime import datetime
import pandas as pd

# Adicionar src ao path
sys.path.append(str(Path(__file__).parent / "src"))

from crypto_etl import run_crypto_etl_pipeline
import config

def print_header():
    """Imprime cabeçalho da demonstração."""
    print("=" * 80)
    print(f"🚀 {config.PROJECT_NAME} ETL - Demonstração")
    print(f"📅 Versão: {config.VERSION}")
    print(f"👤 Autor: {config.AUTHOR}")
    print(f"🕐 Execução: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

def print_footer():
    """Imprime rodapé da demonstração."""
    print("=" * 80)
    print("🎯 Demonstração concluída!")
    print("📊 Este pipeline está pronto para ser integrado ao Apache Airflow")
    print("☁️  Próximos passos: Deploy em AWS + Configuração de DAGs")
    print("=" * 80)

def format_price(price):
    """Formata preço para exibição."""
    return f"${price:,.2f}"

def format_percentage(pct):
    """Formata porcentagem para exibição."""
    sign = "📈" if pct >= 0 else "📉"
    return f"{sign} {pct:+.2f}%"

def show_enhanced_analysis_examples(report_path):
    """
    Mostra exemplos de análises aprimoradas com termos crypto.
    
    Args:
        report_path: Caminho para o arquivo de relatório
    """
    try:
        df = pd.read_csv(report_path)
        
        # Filtrar apenas análises aprimoradas com termos crypto
        if 'analysis_method' in df.columns and 'crypto_terms_found' in df.columns:
            enhanced_rows = df[
                (df['analysis_method'] == 'enhanced_crypto') & 
                (df['crypto_matches'] > 0) &
                (df['sentiment_label'] != 'RESUMO')
            ]
            
            if len(enhanced_rows) > 0:
                print("💎 EXEMPLOS DE ANÁLISE CRYPTO APRIMORADA:")
                print("-" * 50)
                
                # Mostrar até 3 exemplos
                for i, (_, row) in enumerate(enhanced_rows.head(3).iterrows()):
                    emoji = "😊" if row['sentiment_polarity'] > 0.1 else "😞" if row['sentiment_polarity'] < -0.1 else "😐"
                    
                    print(f"{emoji} Exemplo {i+1} (Score: {row['sentiment_polarity']:+.3f}):")
                    print(f"   📝 \"{row['tweet_text'][:80]}...\"")
                    
                    if pd.notna(row['crypto_terms_found']) and row['crypto_terms_found']:
                        terms = row['crypto_terms_found'].replace('[', '').replace(']', '').replace("'", "")
                        print(f"   💎 Termos crypto: {terms}")
                    
                    print(f"   🎯 Confiança: {row['confidence']:.3f}")
                    print()
                
                return True
        
        return False
        
    except Exception as e:
        print(f"❌ Erro ao mostrar exemplos aprimorados: {e}")
        return False

def analyze_report(report_path):
    """
    Analisa o relatório gerado e extrai insights.
    
    Args:
        report_path: Caminho para o arquivo de relatório
        
    Returns:
        Dict com insights do relatório
    """
    try:
        df = pd.read_csv(report_path)
        
        # Filtrar linha de resumo
        data_rows = df[df['sentiment_label'] != 'RESUMO']
        
        if len(data_rows) == 0:
            return {"error": "Nenhum dado encontrado no relatório"}
        
        # Calcular estatísticas
        avg_sentiment = data_rows['sentiment_polarity'].mean()
        positive_posts = len(data_rows[data_rows['sentiment_polarity'] > config.POSITIVE_THRESHOLD])
        negative_posts = len(data_rows[data_rows['sentiment_polarity'] < config.NEGATIVE_THRESHOLD])
        neutral_posts = len(data_rows) - positive_posts - negative_posts
        
        # Estatísticas da análise aprimorada
        enhanced_analyses = len(data_rows[data_rows['analysis_method'] == 'enhanced_crypto']) if 'analysis_method' in data_rows.columns else 0
        crypto_detections = data_rows['crypto_matches'].sum() if 'crypto_matches' in data_rows.columns else 0
        avg_confidence = data_rows['confidence'].mean() if 'confidence' in data_rows.columns else 0
        
        # Post mais positivo e mais negativo
        most_positive_idx = data_rows['sentiment_polarity'].idxmax()
        most_negative_idx = data_rows['sentiment_polarity'].idxmin()
        
        most_positive = data_rows.loc[most_positive_idx] if most_positive_idx is not None else None
        most_negative = data_rows.loc[most_negative_idx] if most_negative_idx is not None else None
        
        insights = {
            "total_posts": len(data_rows),
            "avg_sentiment": avg_sentiment,
            "sentiment_distribution": {
                "positive": positive_posts,
                "negative": negative_posts,
                "neutral": neutral_posts
            },
            "enhanced_stats": {
                "enhanced_analyses": enhanced_analyses,
                "crypto_detections": crypto_detections,
                "avg_confidence": avg_confidence,
                "enhanced_percentage": (enhanced_analyses / len(data_rows) * 100) if len(data_rows) > 0 else 0
            },
            "most_positive": {
                "text": most_positive['tweet_text'][:100] + "..." if most_positive is not None else "N/A",
                "score": most_positive['sentiment_polarity'] if most_positive is not None else 0
            },
            "most_negative": {
                "text": most_negative['tweet_text'][:100] + "..." if most_negative is not None else "N/A",
                "score": most_negative['sentiment_polarity'] if most_negative is not None else 0
            },
            "btc_price": data_rows.iloc[0]['btc_price_usd'],
            "btc_change_24h": data_rows.iloc[0]['btc_change_24h_pct']
        }
        
        return insights
        
    except Exception as e:
        return {"error": f"Erro ao analisar relatório: {e}"}

def main():
    """Função principal da demonstração."""
    print_header()
    
    print("🔄 Iniciando pipeline CryptoSentinel ETL...")
    print()
    
    try:
        # Executar pipeline
        results = run_crypto_etl_pipeline()
        
        if not results.get('success'):
            print(f"❌ Pipeline falhou: {results.get('error', 'Erro desconhecido')}")
            return 1
        
        # Mostrar resultados básicos
        print("✅ Pipeline executado com sucesso!")
        print()
        print("📊 RESUMO DA EXECUÇÃO:")
        print("-" * 40)
        print(f"💰 Preço Bitcoin: {format_price(results['btc_price'])}")
        print(f"📈 Sentimento Médio: {results['avg_sentiment']:.3f}")
        print(f"📝 Posts Analisados: {results['total_texts_analyzed']}")
        print(f"📄 Relatório: {Path(results['report_path']).name}")
        print()
        
        # Analisar relatório em detalhes
        print("🔍 ANÁLISE DETALHADA:")
        print("-" * 40)
        
        insights = analyze_report(results['report_path'])
        
        if "error" in insights:
            print(f"❌ {insights['error']}")
        else:
            # Estatísticas de sentimento
            dist = insights['sentiment_distribution']
            total = insights['total_posts']
            
            print(f"📊 Distribuição de Sentimento:")
            print(f"   😊 Positivos: {dist['positive']} ({dist['positive']/total*100:.1f}%)")
            print(f"   😞 Negativos: {dist['negative']} ({dist['negative']/total*100:.1f}%)")
            print(f"   😐 Neutros: {dist['neutral']} ({dist['neutral']/total*100:.1f}%)")
            print()
            
            # Estatísticas da análise aprimorada
            enhanced_stats = insights['enhanced_stats']
            print(f"⚡ Análise Aprimorada:")
            print(f"   🧠 Análises com método aprimorado: {enhanced_stats['enhanced_analyses']} ({enhanced_stats['enhanced_percentage']:.1f}%)")
            print(f"   💎 Detecções de termos crypto: {enhanced_stats['crypto_detections']}")
            print(f"   🎯 Confiança média: {enhanced_stats['avg_confidence']:.3f}")
            print()
            
            # Interpretar sentimento geral
            avg_sent = insights['avg_sentiment']
            if avg_sent > 0.2:
                sentiment_emoji = "🚀"
                sentiment_desc = "Muito Positivo"
            elif avg_sent > 0.05:
                sentiment_emoji = "📈"
                sentiment_desc = "Positivo"
            elif avg_sent > -0.05:
                sentiment_emoji = "⚖️"
                sentiment_desc = "Neutro"
            elif avg_sent > -0.2:
                sentiment_emoji = "📉"
                sentiment_desc = "Negativo"
            else:
                sentiment_emoji = "💥"
                sentiment_desc = "Muito Negativo"
            
            print(f"🧠 Sentimento Geral: {sentiment_emoji} {sentiment_desc} ({avg_sent:.3f})")
            print()
            
            # Mercado
            btc_change = insights['btc_change_24h']
            print(f"💹 Mercado Bitcoin (24h): {format_percentage(btc_change)}")
            
            # Correlação simples
            if avg_sent > 0 and btc_change > 0:
                correlation = "🟢 Sentimento e preço ambos positivos"
            elif avg_sent < 0 and btc_change < 0:
                correlation = "🔴 Sentimento e preço ambos negativos"
            else:
                correlation = "🟡 Sentimento e preço divergentes"
            
            print(f"🔗 Correlação: {correlation}")
            print()
            
            # Exemplos de posts
            print("📝 EXEMPLOS DE POSTS:")
            print("-" * 40)
            
            if insights['most_positive']['score'] > 0:
                print(f"😊 Mais Positivo ({insights['most_positive']['score']:.3f}):")
                print(f"   \"{insights['most_positive']['text']}\"")
                print()
            
            if insights['most_negative']['score'] < 0:
                print(f"😞 Mais Negativo ({insights['most_negative']['score']:.3f}):")
                print(f"   \"{insights['most_negative']['text']}\"")
                print()
        
        # Mostrar exemplos de análise aprimorada
        show_enhanced_analysis_examples(results['report_path'])
        
        # Localização dos arquivos
        print("📁 ARQUIVOS GERADOS:")
        print("-" * 40)
        print(f"📄 Relatório CSV: {results['report_path']}")
        
        if config.LOGS_DIR.exists():
            log_files = list(config.LOGS_DIR.glob("*.log"))
            if log_files:
                print(f"📋 Logs: {log_files[0]}")
        
        print()
        
        # Próximos passos
        print("🎯 PRÓXIMOS PASSOS:")
        print("-" * 40)
        print("1. 🔧 Implementar DAG do Airflow")
        print("2. ☁️  Configurar integração AWS S3")
        print("3. 📊 Criar dashboard de visualização")
        print("4. 🚨 Implementar alertas automáticos")
        print("5. 📈 Adicionar métricas de performance")
        
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
        return 1
    
    finally:
        print()
        print_footer()
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)