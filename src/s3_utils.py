"""
Utilitário para integração com Amazon S3.

Este módulo fornece funcionalidades para upload e gerenciamento
de arquivos no S3 de forma segura e eficiente.

Autor: Vitória
Data: 2025-11-02
"""

import boto3
import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
from botocore.exceptions import ClientError, NoCredentialsError

logger = logging.getLogger(__name__)


class S3Manager:
    """
    Classe para gerenciar operações com Amazon S3.
    """
    
    def __init__(self, bucket_name: str = "crypto-sentinel-reports", region: str = "us-east-1"):
        """
        Inicializa o gerenciador S3.
        
        Args:
            bucket_name: Nome do bucket S3
            region: Região AWS
        """
        self.bucket_name = bucket_name
        self.region = region
        
        try:
            # Inicializar cliente S3
            self.s3_client = boto3.client('s3', region_name=region)
            self.s3_resource = boto3.resource('s3', region_name=region)
            
            # Verificar se o bucket existe
            self._verify_bucket_access()
            
        except NoCredentialsError:
            logger.error("❌ Credenciais AWS não encontradas!")
            raise Exception("Configure as credenciais AWS (aws configure ou variáveis de ambiente)")
        except Exception as e:
            logger.error(f"❌ Erro ao inicializar S3Manager: {e}")
            raise
    
    def _verify_bucket_access(self) -> bool:
        """
        Verifica se o bucket existe e se temos acesso.
        
        Returns:
            True se o bucket está acessível
        """
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"✅ Bucket S3 acessível: {self.bucket_name}")
            return True
        except ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                logger.error(f"❌ Bucket não encontrado: {self.bucket_name}")
                raise Exception(f"Bucket {self.bucket_name} não existe")
            elif error_code == 403:
                logger.error(f"❌ Sem permissão para acessar bucket: {self.bucket_name}")
                raise Exception(f"Sem permissão para bucket {self.bucket_name}")
            else:
                logger.error(f"❌ Erro ao verificar bucket: {e}")
                raise
    
    def upload_report(self, local_file_path: str, s3_key: Optional[str] = None) -> Dict[str, Any]:
        """
        Faz upload de um relatório para o S3.
        
        Args:
            local_file_path: Caminho local do arquivo
            s3_key: Chave S3 customizada (opcional)
            
        Returns:
            Dict com informações do upload
        """
        local_path = Path(local_file_path)
        
        if not local_path.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {local_file_path}")
        
        # Gerar chave S3 se não fornecida
        if not s3_key:
            filename = local_path.name
            date_str = datetime.now().strftime("%Y/%m/%d")
            s3_key = f"reports/daily/{date_str}/{filename}"
        
        try:
            # Fazer upload
            logger.info(f"📤 Uploading para S3: {s3_key}")
            
            extra_args = {
                'ServerSideEncryption': 'AES256',  # Criptografia
                'Metadata': {
                    'source': 'crypto-sentinel-etl',
                    'uploaded_at': datetime.now().isoformat(),
                    'file_size': str(local_path.stat().st_size)
                }
            }
            
            self.s3_client.upload_file(
                str(local_path), 
                self.bucket_name, 
                s3_key,
                ExtraArgs=extra_args
            )
            
            # Gerar URLs
            s3_url = f"s3://{self.bucket_name}/{s3_key}"
            https_url = f"https://{self.bucket_name}.s3.{self.region}.amazonaws.com/{s3_key}"
            
            upload_info = {
                'success': True,
                's3_url': s3_url,
                'https_url': https_url,
                's3_key': s3_key,
                'bucket': self.bucket_name,
                'file_size': local_path.stat().st_size,
                'uploaded_at': datetime.now().isoformat()
            }
            
            logger.info(f"✅ Upload concluído: {s3_url}")
            return upload_info
            
        except Exception as e:
            logger.error(f"❌ Erro no upload para S3: {e}")
            return {
                'success': False,
                'error': str(e),
                's3_key': s3_key
            }
    
    def upload_logs(self, log_file_path: str) -> Dict[str, Any]:
        """
        Faz upload de logs para o S3.
        
        Args:
            log_file_path: Caminho do arquivo de log
            
        Returns:
            Dict com informações do upload
        """
        log_path = Path(log_file_path)
        if not log_path.exists():
            logger.warning(f"⚠️ Log não encontrado: {log_file_path}")
            return {'success': False, 'error': 'Log file not found'}
        
        # Chave S3 para logs
        timestamp = datetime.now().strftime("%Y/%m/%d")
        s3_key = f"logs/{timestamp}/{log_path.name}"
        
        return self.upload_report(log_file_path, s3_key)
    
    def list_reports(self, prefix: str = "reports/", max_items: int = 50) -> List[Dict[str, Any]]:
        """
        Lista relatórios no S3.
        
        Args:
            prefix: Prefixo para filtrar objetos
            max_items: Número máximo de itens
            
        Returns:
            Lista de relatórios
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=max_items
            )
            
            reports = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    reports.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'].isoformat(),
                        's3_url': f"s3://{self.bucket_name}/{obj['Key']}"
                    })
            
            logger.info(f"📋 Encontrados {len(reports)} relatórios")
            return reports
            
        except Exception as e:
            logger.error(f"❌ Erro ao listar relatórios: {e}")
            return []
    
    def download_report(self, s3_key: str, local_path: str) -> bool:
        """
        Faz download de um relatório do S3.
        
        Args:
            s3_key: Chave do objeto no S3
            local_path: Caminho local para salvar
            
        Returns:
            True se download foi bem-sucedido
        """
        try:
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            logger.info(f"📥 Download concluído: {local_path}")
            return True
        except Exception as e:
            logger.error(f"❌ Erro no download: {e}")
            return False
    
    def create_bucket_if_not_exists(self) -> bool:
        """
        Cria o bucket S3 se ele não existir.
        
        Returns:
            True se bucket foi criado ou já existe
        """
        try:
            # Verificar se já existe
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"✅ Bucket já existe: {self.bucket_name}")
            return True
            
        except ClientError as e:
            if int(e.response['Error']['Code']) == 404:
                # Bucket não existe, criar
                try:
                    if self.region == 'us-east-1':
                        # us-east-1 não precisa de LocationConstraint
                        self.s3_client.create_bucket(Bucket=self.bucket_name)
                    else:
                        self.s3_client.create_bucket(
                            Bucket=self.bucket_name,
                            CreateBucketConfiguration={'LocationConstraint': self.region}
                        )
                    
                    # Configurar bloqueio de acesso público
                    self.s3_client.put_public_access_block(
                        Bucket=self.bucket_name,
                        PublicAccessBlockConfiguration={
                            'BlockPublicAcls': True,
                            'IgnorePublicAcls': True,
                            'BlockPublicPolicy': True,
                            'RestrictPublicBuckets': True
                        }
                    )
                    
                    logger.info(f"✅ Bucket criado com sucesso: {self.bucket_name}")
                    return True
                    
                except Exception as create_error:
                    logger.error(f"❌ Erro ao criar bucket: {create_error}")
                    return False
            else:
                logger.error(f"❌ Erro ao verificar bucket: {e}")
                return False


def test_s3_connection(bucket_name: str = "crypto-sentinel-reports") -> bool:
    """
    Testa a conexão com S3.
    
    Args:
        bucket_name: Nome do bucket para testar
        
    Returns:
        True se conexão está funcionando
    """
    try:
        s3_manager = S3Manager(bucket_name)
        
        # Teste básico: listar objetos
        reports = s3_manager.list_reports(max_items=1)
        
        logger.info("✅ Teste de conexão S3 bem-sucedido!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Teste de conexão S3 falhou: {e}")
        return False


if __name__ == "__main__":
    # Teste do módulo
    logging.basicConfig(level=logging.INFO)
    
    print("🧪 Testando conexão S3...")
    if test_s3_connection():
        print("✅ S3 configurado corretamente!")
    else:
        print("❌ Erro na configuração S3!")