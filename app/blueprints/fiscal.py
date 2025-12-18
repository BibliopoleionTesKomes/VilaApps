# --- IMPORTAÇÕES ---
# Flask: Para criar rotas e renderizar páginas HTML.
from flask import Blueprint, render_template

# Serviços: 
# 'ler_cache': Função crucial. Ela busca os dados que o "robô" (thread em background)
# preparou e salvou. Isso faz a página carregar instantaneamente, sem esperar consultas SQL demoradas.
from app.services.cache_service import ler_cache

# Repositórios:
# '_carregar_workflow_local': Lê o arquivo JSON onde salvamos os status manuais (Pendente, Em Análise, etc).
from app.repository.gestao_repo import _carregar_workflow_local

# Criação do Blueprint 'fiscal'
fiscal_bp = Blueprint('fiscal', __name__)

# --- ROTAS (PÁGINAS) ---

@fiscal_bp.route('/acerto')
def acerto():
    """
    Tela de Validação de Notas de Acerto.
    Exibe o cruzamento entre XMLs de entrada e Pedidos do ERP.
    """
    # 1. Busca os dados prontos no Cache
    # 'dados': Lista de dicionários com as informações das notas.
    # 'ts': Timestamp (data/hora) da última vez que o robô rodou.
    dados, ts = ler_cache('acerto')
    
    # 2. Enriquecimento de Dados (Data Enrichment)
    # Se houver dados, vamos adicionar o 'Status' atual de cada nota.
    if dados:
        # Carrega o "bancozinho" local de status
        db_st = _carregar_workflow_local()
        
        for r in dados:
            # A chave única é a chave de acesso da NFe
            chave = r.get('Chave_Acesso')
            
            # Busca o status salvo para essa chave. Se não achar, retorna dicionário vazio.
            entry = db_st.get(chave, {})
            
            # Tratamento de legado: Se o dado antigo era só uma string, converte para objeto
            if isinstance(entry, str): 
                entry = {'status': entry}
            
            # Adiciona o campo 'Status_Workflow' ao dicionário da nota.
            # Se não tiver status salvo, assume 'PENDENTE'.
            r['Status_Workflow'] = entry.get('status', 'PENDENTE')
            
    # 3. Renderiza a página
    return render_template('acerto.html', dados=dados, ultima_atualizacao=ts)

@fiscal_bp.route('/devolucao')
def devolucao():
    """
    Tela de Validação de Notas de Devolução.
    Lógica similar à de Acerto, mas lendo um cache diferente ('devolucao').
    """
    # Lê o cache específico de devoluções
    dados, ts = ler_cache('devolucao')
    
    # Nota: Aqui não estamos enriquecendo com status (pelo código original),
    # mas poderíamos adicionar a mesma lógica do 'acerto' se necessário futuramente.
    
    return render_template('devolucao.html', dados=dados, ultima_atualizacao=ts)