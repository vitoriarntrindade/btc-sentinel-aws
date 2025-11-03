"""
DAG do Apache Airflow para o pipeline CryptoSentinel ETL.

Esta DAG orquestra a execução automatizada do pipeline de análise
de sentimento sobre Bitcoin, incluindo upload para S3.

Autor: Vitória
Data: 2025-11-02
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
import sys
import os
import logging

# Adicionar path do nosso código (ajustar conforme sua estrutura no EC2)
sys.path.append('/home/ec2-user/airflow/plugins')
sys.path.append('/home/ec2-user/airflow/plugins/crypto_sentinel')

# Importar nossos módulos
try:
    from crypto_sentinel.crypto_etl import run_crypto_etl_pipeline
    from crypto_sentinel.s3_utils import S3Manager, test_s3_connection
except ImportError as e:
    logging.error(f"Erro ao importar módulos: {e}")
    # Fallback para desenvolvimento local
    sys.path.append('/path/to/your/local/src')  # Ajustar se necessário

# Configurações da DAG
default_args = {
    'owner': 'vitoria',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 2, 6, 0),  # Começar às 6h
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
    'email': ['your-email@example.com'],  # ⚠️ SUBSTITUIR pelo seu email
    'catchup': False
}

# Definir DAG
dag = DAG(
    'crypto_sentinel_etl',
    default_args=default_args,
    description='🛡️ CryptoSentinel ETL - Análise de sentimento Bitcoin com upload S3',
    schedule_interval='0 */6 * * *',  # A cada 6 horas: 00:00, 06:00, 12:00, 18:00
    max_active_runs=1,  # Apenas uma execução por vez
    catchup=False,
    tags=['crypto', 'etl', 'sentiment-analysis', 'bitcoin', 'nlp']
)

# ==========================================
# FUNÇÕES DAS TAREFAS
# ==========================================

def check_dependencies(**context):
    """
    Verifica se todas as dependências estão disponíveis.
    """
    logging.info("🔍 Verificando dependências...")
    
    try:
        # Testar imports
        import pandas
        import textblob
        import requests
        import boto3
        
        # Testar conexão S3
        if not test_s3_connection():
            raise Exception("Falha na conexão com S3")
        
        logging.info("✅ Todas as dependências verificadas com sucesso!")
        return {"status": "success", "message": "Dependencies OK"}
        
    except Exception as e:
        logging.error(f"❌ Erro na verificação de dependências: {e}")
        raise


def execute_crypto_pipeline(**context):
    """
    Executa o pipeline principal do CryptoSentinel.
    """
    logging.info("🚀 Iniciando pipeline CryptoSentinel...")
    
    try:
        # Executar pipeline com upload S3
        results = run_crypto_etl_pipeline(upload_to_s3=True)
        
        if not results.get('success'):
            error_msg = results.get('error', 'Erro desconhecido')
            logging.error(f"❌ Pipeline falhou: {error_msg}")
            raise Exception(f"Pipeline execution failed: {error_msg}")
        
        # Log dos resultados
        logging.info("✅ Pipeline executado com sucesso!")
        logging.info(f"💰 Preço Bitcoin: ${results['btc_price']:,.2f}")
        logging.info(f"🧠 Sentimento médio: {results['avg_sentiment']:.3f}")
        logging.info(f"📊 Textos analisados: {results['total_texts_analyzed']}")
        
        if 's3_url' in results:
            logging.info(f"☁️ Relatório S3: {results['s3_url']}")
        
        # Retornar via XCom para próximas tarefas
        return results
        
    except Exception as e:
        logging.error(f"❌ Erro na execução do pipeline: {e}")
        raise


def validate_results(**context):
    """
    Valida os resultados do pipeline.
    """
    # Recuperar resultados da tarefa anterior
    results = context['task_instance'].xcom_pull(task_ids='execute_pipeline')
    
    if not results:
        raise Exception("Nenhum resultado recebido do pipeline")
    
    # Validações
    validations = []
    
    # 1. Preço Bitcoin válido
    btc_price = results.get('btc_price', 0)
    if btc_price > 1000:  # Bitcoin > $1000 (sanity check)
        validations.append("✅ Preço Bitcoin válido")
    else:
        validations.append(f"⚠️ Preço Bitcoin suspeito: ${btc_price}")
    
    # 2. Número de textos analisados
    total_texts = results.get('total_texts_analyzed', 0)
    if total_texts >= 5:  # Mínimo 5 textos
        validations.append("✅ Quantidade adequada de textos")
    else:
        validations.append(f"⚠️ Poucos textos analisados: {total_texts}")
    
    # 3. Sentimento dentro de range esperado
    avg_sentiment = results.get('avg_sentiment', 0)
    if -1 <= avg_sentiment <= 1:  # Range válido do sentimento
        validations.append("✅ Sentimento dentro do range esperado")
    else:
        validations.append(f"⚠️ Sentimento fora do range: {avg_sentiment}")
    
    # 4. Upload S3 bem-sucedido
    if results.get('s3_url'):
        validations.append("✅ Upload S3 realizado")
    else:
        validations.append("⚠️ Upload S3 não confirmado")
    
    # Log das validações
    for validation in validations:
        logging.info(validation)
    
    # Falhar se muitas validações falharam
    warnings = [v for v in validations if v.startswith("⚠️")]
    if len(warnings) > 2:
        raise Exception(f"Muitas validações falharam: {warnings}")
    
    logging.info("🎯 Validação concluída com sucesso!")
    return {"validations": validations, "warnings": len(warnings)}


def generate_summary_email(**context):
    """
    Gera resumo por email dos resultados.
    """
    # Recuperar dados das tarefas anteriores
    results = context['task_instance'].xcom_pull(task_ids='execute_pipeline')
    validation = context['task_instance'].xcom_pull(task_ids='validate_results')
    
    execution_date = context['execution_date'].strftime('%Y-%m-%d %H:%M:%S UTC')
    
    # Determinar status geral
    warnings = validation.get('warnings', 0) if validation else 0
    status_emoji = "🎉" if warnings == 0 else "⚠️" if warnings <= 2 else "❌"
    
    # Construir email
    subject = f"{status_emoji} CryptoSentinel ETL - Relatório {context['ds']}"
    
    html_content = f"""
    <h2>🛡️ CryptoSentinel ETL - Relatório de Execução</h2>
    
    <p><strong>📅 Data/Hora:</strong> {execution_date}</p>
    <p><strong>🎯 Status:</strong> {status_emoji} {'Sucesso' if warnings == 0 else 'Sucesso com avisos' if warnings <= 2 else 'Falha'}</p>
    
    <h3>📊 Resultados Principais</h3>
    <ul>
        <li><strong>💰 Preço Bitcoin:</strong> ${results.get('btc_price', 0):,.2f} USD</li>
        <li><strong>🧠 Sentimento Médio:</strong> {results.get('avg_sentiment', 0):.3f}</li>
        <li><strong>📝 Textos Analisados:</strong> {results.get('total_texts_analyzed', 0)}</li>
        <li><strong>☁️ Relatório S3:</strong> {'✅ Enviado' if results.get('s3_url') else '❌ Falha'}</li>
    </ul>
    
    <h3>🔍 Validações</h3>
    <ul>
    """
    
    for val in validation.get('validations', []):
        html_content += f"<li>{val}</li>"
    
    html_content += f"""
    </ul>
    
    <h3>🔗 Links</h3>
    <ul>
        <li><a href="http://your-ec2-ip:8080/tree?dag_id=crypto_sentinel_etl">Ver DAG no Airflow</a></li>
        <li><a href="https://s3.console.aws.amazon.com/s3/buckets/crypto-sentinel-reports">Bucket S3</a></li>
    </ul>
    
    <hr>
    <p><small>Gerado automaticamente pelo CryptoSentinel ETL | Airflow DAG: crypto_sentinel_etl</small></p>
    """
    
    return {
        'subject': subject,
        'html_content': html_content,
        'to': default_args['email']
    }


def cleanup_old_files(**context):
    """
    Limpeza de arquivos antigos (opcional).
    """
    logging.info("🧹 Executando limpeza de arquivos antigos...")
    
    try:
        # Exemplo: remover relatórios locais com mais de 7 dias
        import glob
        from datetime import datetime, timedelta
        
        # Path dos relatórios locais (ajustar conforme necessário)
        reports_path = "/home/ec2-user/airflow/data/reports"
        
        if os.path.exists(reports_path):
            cutoff_date = datetime.now() - timedelta(days=7)
            
            for file_path in glob.glob(f"{reports_path}/*.csv"):
                file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                if file_time < cutoff_date:
                    os.remove(file_path)
                    logging.info(f"🗑️ Arquivo removido: {file_path}")
        
        logging.info("✅ Limpeza concluída!")
        return {"status": "cleanup_completed"}
        
    except Exception as e:
        logging.warning(f"⚠️ Erro na limpeza: {e}")
        return {"status": "cleanup_failed", "error": str(e)}


# ==========================================
# DEFINIR TAREFAS
# ==========================================

task_check_deps = PythonOperator(
    task_id='check_dependencies',
    python_callable=check_dependencies,
    dag=dag,
    doc_md="""
    ## Verificação de Dependências
    
    Esta tarefa verifica se todas as dependências estão disponíveis:
    - Bibliotecas Python (pandas, textblob, requests, boto3)
    - Conectividade com S3
    - Credenciais AWS
    """
)

task_execute_pipeline = PythonOperator(
    task_id='execute_pipeline',
    python_callable=execute_crypto_pipeline,
    dag=dag,
    doc_md="""
    ## Execução do Pipeline Principal
    
    Executa o pipeline completo CryptoSentinel:
    1. Coleta preços Bitcoin (CoinGecko API)
    2. Coleta posts sobre Bitcoin (RSS feeds)
    3. Análise de sentimento com método aprimorado
    4. Geração de relatório CSV
    5. Upload para S3
    """
)

task_validate_results = PythonOperator(
    task_id='validate_results',
    python_callable=validate_results,
    dag=dag,
    doc_md="""
    ## Validação dos Resultados
    
    Valida a qualidade dos dados processados:
    - Preço Bitcoin dentro de range esperado
    - Quantidade mínima de textos analisados
    - Sentimento dentro de limites válidos
    - Confirmação de upload S3
    """
)

task_summary_email = PythonOperator(
    task_id='generate_summary',
    python_callable=generate_summary_email,
    dag=dag,
    doc_md="""
    ## Geração de Resumo
    
    Gera resumo executivo da execução para notificação.
    """
)

task_cleanup = PythonOperator(
    task_id='cleanup_old_files',
    python_callable=cleanup_old_files,
    dag=dag,
    trigger_rule='all_done',  # Executar sempre, mesmo se outras tarefas falharam
    doc_md="""
    ## Limpeza de Arquivos
    
    Remove arquivos antigos para economizar espaço em disco.
    Executa sempre, independente do status das outras tarefas.
    """
)

# ==========================================
# DEFINIR DEPENDÊNCIAS
# ==========================================

# Fluxo principal
task_check_deps >> task_execute_pipeline >> task_validate_results >> task_summary_email

# Limpeza paralela (independente)
task_validate_results >> task_cleanup

# ==========================================
# DOCUMENTAÇÃO DA DAG
# ==========================================

dag.doc_md = """
# 🛡️ CryptoSentinel ETL Pipeline

Pipeline automatizado de análise de sentimento sobre Bitcoin que combina:

## 🎯 Funcionalidades
- **Coleta de Dados**: Preços Bitcoin + Posts sobre crypto
- **Análise Avançada**: Sentimento com dicionário crypto especializado  
- **Armazenamento**: Upload automático para S3
- **Monitoramento**: Validações e notificações por email

## ⏰ Execução
- **Frequência**: A cada 6 horas (00:00, 06:00, 12:00, 18:00)
- **Duração Média**: 2-5 minutos
- **Retry**: 2 tentativas com intervalo de 15 minutos

## 📊 Outputs
- Relatórios CSV no S3: `s3://crypto-sentinel-reports/reports/daily/`
- Logs detalhados no Airflow
- Email de resumo para stakeholders

## 🚨 Alertas
- Email automático em caso de falha
- Validações de qualidade dos dados
- Monitoramento de métricas principais

## 👥 Contato
**Desenvolvido por:** Vitória  
**Email:** vitoriarntrindade@gmail.com
"""
