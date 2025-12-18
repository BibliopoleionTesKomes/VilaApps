# --- IMPORTAÇÕES ---
# Flask: Ferramentas web padrão.
from flask import Blueprint, render_template, request, jsonify
# Datetime: Para calcular datas padrão (ex: últimos 30 dias).
from datetime import datetime, timedelta

# Repositórios: Funções que buscam dados no SQL ou arquivos locais.
from app.repository.gestao_repo import (
    listar_propostas_gestao, 
    buscar_detalhes_proposta, 
    adicionar_historico_repo, 
    excluir_historico_repo, 
    buscar_contato_fornecedor, 
    _carregar_workflow_local
)
# Serviços: Cache para carregar dados processados pelo robô.
from app.services.cache_service import ler_cache
from app.repository.geral_repo import buscar_filiais

# Criação do Blueprint 'gestao'
gestao_bp = Blueprint('gestao', __name__)

# --- ROTA PRINCIPAL: PAINEL DE GESTÃO ---

@gestao_bp.route('/')
def index():
    """
    Tela principal de Gestão de Propostas.
    Lista as propostas com base nos filtros da URL (GET).
    """
    # 1. Configuração de Datas Padrão (Últimos 30 dias se não informado)
    hoje = datetime.now()
    inicio = hoje - timedelta(days=30)
    
    # request.args.get: Pega o valor da URL ou usa o padrão definido
    data_ini = request.args.get('data_ini', inicio.strftime('%Y-%m-%d'))
    data_fim = request.args.get('data_fim', hoje.strftime('%Y-%m-%d'))
    status = request.args.get('status', 'todos')
    
    # Filtros de texto opcionais
    filtro_proposta = request.args.get('filtro_proposta', '')
    filtro_fornecedor = request.args.get('filtro_fornecedor', '')
    filtro_filial = request.args.get('filtro_filial', '')

    # 2. Busca no Banco de Dados
    propostas = listar_propostas_gestao(
        data_ini, data_fim, status, 
        filtro_proposta, filtro_fornecedor, filtro_filial
    )

    # 3. Renderiza o HTML passando os dados e os filtros atuais (para manter os campos preenchidos)
    return render_template('gestao_propostas.html', 
                           propostas=propostas, 
                           filtros={
                               'ini': data_ini, 
                               'fim': data_fim, 
                               'status': status, 
                               'proposta': filtro_proposta, 
                               'fornecedor': filtro_fornecedor, 
                               'filial': filtro_filial
                           })

# --- ROTA SECUNDÁRIA: WORKFLOW (FOLLOW-UP) ---

@gestao_bp.route('/workflow')
def workflow():
    """
    Tela de Workflow / Follow-up.
    Mostra apenas as notas que estão 'Em Análise' ou 'Concluídas'.
    """
    # 1. Carrega dados do Cache Geral (mesmo usado no Leitor XML)
    dados, ts = ler_cache('geral')
    
    # 2. Carrega o banco local de status (arquivo JSON)
    db_st = _carregar_workflow_local()
    
    lista_filtrada = []
    lojas = []
    
    # Tenta carregar lista de lojas para o filtro lateral
    try:
        df = buscar_filiais()
        if not df.empty: lojas = sorted(df['Nome_Filial'].unique().tolist())
    except: 
        pass

    # 3. Filtragem e Enriquecimento
    if dados:
        for r in dados:
            chave = r.get('Chave_Acesso')
            
            # Busca dados salvos localmente (Status, Motivo, Obs)
            entry = db_st.get(chave, {})
            # Compatibilidade com versão antiga (se era string, vira dict)
            if isinstance(entry, str): entry = {'status': entry}
            
            # Preenche o dicionário da nota com os dados do workflow
            r['Status_Workflow'] = entry.get('status', 'PENDENTE')
            r['Motivo'] = entry.get('motivo', '')
            r['Observacao'] = entry.get('observacao', '')
            r['Tratativa'] = entry.get('tratativa', '')
            r['Data_Contato'] = entry.get('data_contato', '')
            r['Responsavel'] = entry.get('responsavel', '')
            
            # Só adiciona à lista se NÃO for Pendente
            if r['Status_Workflow'] in ['EM ANÁLISE', 'CONCLUÍDO']: 
                lista_filtrada.append(r)
                
    return render_template('workflow.html', dados=lista_filtrada, ultima_atualizacao=ts, lista_lojas=lojas)

# --- ROTAS DE API (JSON) PARA OS MODAIS ---

@gestao_bp.route('/api/itens/<pedido>')
def api_itens(pedido):
    """
    Busca os ITENS de uma proposta específica (detalhe).
    Usado no modal de 'Ver Itens'.
    """
    return jsonify(buscar_detalhes_proposta(pedido))

@gestao_bp.route('/api/contato/<cod_cli>')
def api_contato(cod_cli):
    """
    Busca os dados de contato (email/telefone) do fornecedor.
    Usado no modal de 'Contato'.
    """
    return jsonify(buscar_contato_fornecedor(cod_cli))

# --- ROTAS DE AÇÃO (SALVAR/EXCLUIR HISTÓRICO) ---

@gestao_bp.route('/api/adicionar_historico', methods=['POST'])
def api_adicionar_historico():
    """
    Salva uma nova mensagem no histórico do workflow.
    """
    try:
        p = request.json
        if adicionar_historico_repo(p.get('chave'), p.get('responsavel'), p.get('data'), p.get('obs')):
            return jsonify({'success': True})
        return jsonify({'success': False, 'msg': 'Erro ao salvar'})
    except Exception as e: 
        return jsonify({'success': False, 'msg': str(e)})

@gestao_bp.route('/api/excluir_historico', methods=['POST'])
def api_excluir_historico():
    """
    Remove uma mensagem do histórico.
    """
    try:
        p = request.json
        # O índice vem do front-end para saber qual mensagem apagar da lista
        if excluir_historico_repo(p.get('chave'), int(p.get('index'))):
            return jsonify({'success': True})
        return jsonify({'success': False, 'msg': 'Erro ao excluir'})
    except Exception as e: 
        return jsonify({'success': False, 'msg': str(e)})
    
@gestao_bp.route('/api/atualizar_dados_extras', methods=['POST'])
def api_atualizar_dados_extras():
    """
    Salva campos extras do workflow (Motivo, Observação, Tratativa, Data Contato, Responsável)
    sem recarregar a página (AJAX).
    """
    try:
        p = request.json
        chave = p.get('chave')
        campo = p.get('campo') # Ex: 'motivo', 'observacao'
        valor = p.get('valor')
        
        # Carrega, Atualiza e Salva
        db = _carregar_workflow_local()
        
        if chave not in db: db[chave] = {}
        if isinstance(db[chave], str): db[chave] = {'status': db[chave]} # Migração legado
            
        # Mapeamento de nomes de campos (HTML -> JSON)
        mapa_campos = {
            'Motivo': 'motivo',
            'Observacao': 'observacao',
            'Tratativa': 'tratativa',
            'Data_Contato': 'data_contato',
            'Responsavel': 'responsavel',
            'Status_Workflow': 'status'
        }
        
        campo_json = mapa_campos.get(campo)
        if campo_json:
            db[chave][campo_json] = valor
            
            # Repositório deve ter a função de salvar (importar se não estiver no topo)
            from app.repository.gestao_repo import _salvar_workflow_local
            _salvar_workflow_local(db)
            
            return jsonify({'success': True})
            
        return jsonify({'success': False, 'msg': 'Campo inválido'})
    except Exception as e:
        return jsonify({'success': False, 'msg': str(e)})