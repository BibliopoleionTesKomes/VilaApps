# --- IMPORTAÇÕES ---
import os           # Para verificar se arquivos existem e manipular caminhos
import json         # Para ler e gravar arquivos no formato JSON (texto estruturado)
import tempfile     # Para encontrar a pasta temporária do sistema (ex: /tmp no Linux ou %TEMP% no Windows)

# --- CONFIGURAÇÃO DOS CAMINHOS ---
# Definimos onde os arquivos de cache serão salvos.
# Usamos 'tempfile.gettempdir()' para garantir que funcione em qualquer computador,
# pois ele busca automaticamente a pasta temporária correta do sistema operacional.
CACHE_ACERTO = os.path.join(tempfile.gettempdir(), 'vila_cache_acerto.json')
CACHE_DEVOLUCAO = os.path.join(tempfile.gettempdir(), 'vila_cache_devolucao.json')
CACHE_GERAL = os.path.join(tempfile.gettempdir(), 'vila_cache_geral.json')

def ler_cache(tipo='geral'):
    """
    Função genérica para ler dados do cache.
    
    Args:
        tipo (str): Pode ser 'geral', 'acerto' ou 'devolucao'. 
                    Define qual arquivo vamos abrir.
    
    Returns:
        tuple: Retorna dois valores (dados, timestamp).
               - dados: A lista de registros.
               - timestamp: A data/hora da última atualização.
    """
    
    # 1. Mapa de Seleção
    # Um dicionário simples para escolher o arquivo certo baseado no 'tipo' pedido.
    mapa = {
        'geral': CACHE_GERAL,
        'acerto': CACHE_ACERTO,
        'devolucao': CACHE_DEVOLUCAO
    }
    
    # Pega o caminho do arquivo no mapa. Se o tipo não existir, usa o GERAL por segurança.
    arquivo = mapa.get(tipo, CACHE_GERAL)
    
    # 2. Verificação de Existência
    # Antes de tentar abrir, perguntamos ao sistema operacional: "Esse arquivo existe?"
    if os.path.exists(arquivo):
        try:
            # 3. Leitura do Arquivo
            # 'with open' é a maneira segura de abrir arquivos em Python.
            # Ele garante que o arquivo será fechado automaticamente depois de ler,
            # mesmo que dê erro no meio do caminho.
            with open(arquivo, 'r', encoding='utf-8') as f:
                # O json.load converte o texto do arquivo de volta para Dicionários/Listas do Python
                c = json.load(f)
                
                # Retorna os dados encontrados e a hora que foi salvo.
                # O .get() é usado para evitar erro se a chave não existir (retorna padrão [] ou '-')
                return c.get('dados', []), c.get('timestamp', '-')
                
        except Exception as e:
            # Se o arquivo estiver corrompido ou ilegível, não travamos o site.
            # Apenas retornamos vazio e seguimos a vida.
            print(f"Erro ao ler cache ({tipo}): {e}")
            return [], None
            
    # Se o arquivo não existir (primeira vez rodando), retorna vazio.
    return [], None