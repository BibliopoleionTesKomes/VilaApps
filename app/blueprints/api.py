from flask import Blueprint, jsonify, request, current_app
import threading

# Importações dos repositórios
from app.repository.geral_repo import (
    listar_fornecedores, 
    listar_filiais_do_fornecedor, 
    listar_pedidos_do_fornecedor, 
    buscar_pedido_manual
)
from app.repository.gestao_repo import (
    buscar_contato_fornecedor, 
    _salvar_workflow_local, 
    _carregar_workflow_local
)

# Importações do serviço de processamento (lógica pesada que roda em segundo plano)
from app.services.processamento_service import STATUS_GLOBAL, resetar_progresso, tarefa_background
from config import Config

# Criação do Blueprint 'api'
# Blueprints ajudam a organizar a aplicação em módulos.
# Tudo que for definido aqui será acessível via prefixo definido no create_app (ex: /api)
api_bp = Blueprint('api', __name__)

# --- FUNÇÃO WRAPPER PARA THREAD (SOLUÇÃO DO ERRO DE CONTEXTO) ---
def executor_thread(app_real, modulo, app_config):
    """
    Esta função roda dentro da Thread.
    Ela 'empurra' o contexto da aplicação para que o database.py 
    consiga ler a string de conexão sem dar erro.
    """
    with app_real.app_context():
        tarefa_background(modulo, app_config)

# --- ROTAS DE PROCESSAMENTO (TAREFAS DEMORADAS) ---

@api_bp.route('/iniciar_processamento', methods=['POST'])
def api_iniciar():
    if STATUS_GLOBAL['status'] == 'rodando':
        return jsonify({'status': 'ocupado'})
    
    resetar_progresso()
    
    modulo = request.json.get('modulo', 'geral')
    
    # 1. Captura a aplicação real (não o proxy) para passar à thread
    app_real = current_app._get_current_object()
    
    # 2. Prepara as configs manuais (backup)
    app_config = {
        'CAMINHO_XML_PADRAO': Config.CAMINHO_XML_PADRAO,
        'PASTAS_IGNORADAS': Config.PASTAS_IGNORADAS,
        'CFOPS_PADRAO': Config.CFOPS_PADRAO
    }
    
    # 3. Inicia a Thread passando a APP REAL para o executor
    thread = threading.Thread(target=executor_thread, args=(app_real, modulo, app_config))
    thread.start()
    
    return jsonify({'status': 'iniciado'})

@api_bp.route('/progresso')
def api_progresso():
    """
    Rota que o Front-end chama repetidamente (Polling) para atualizar a barra de progresso.
    Lê a variável global STATUS_GLOBAL que a Thread está atualizando.
    """
    pct = 0
    # Evita divisão por zero
    if STATUS_GLOBAL['total'] > 0:
        pct = int((STATUS_GLOBAL['atual'] / STATUS_GLOBAL['total']) * 100)
    
    return jsonify({
        'atual': STATUS_GLOBAL['atual'], 
        'total': STATUS_GLOBAL['total'],
        'percentual': pct, 
        'status': STATUS_GLOBAL['status'], 
        'msg': STATUS_GLOBAL['msg']
    })

# --- ROTAS DE CONSULTA BÁSICA ---

@api_bp.route('/fornecedores')
def api_fornecedores():
    tipo = request.args.get('tipo', 1) 
    return jsonify(listar_fornecedores(tipo))

@api_bp.route('/filiais')
def api_filiais():
    cod_cliente = request.args.get('cod_cli')
    tipo = request.args.get('tipo', 1)
    return jsonify(listar_filiais_do_fornecedor(cod_cliente, tipo))

@api_bp.route('/pedidos')
def api_pedidos():
    a = request.args
    dados = listar_pedidos_do_fornecedor(
        a.get('cod_cli'), a.get('cod_filial'), 
        a.get('data_ini'), a.get('data_fim'), a.get('tipo', 1)
    )
    return jsonify(dados)

@api_bp.route('/buscar_pedido')
def api_buscar_pedido():
    pedido = request.args.get('pedido')
    tipo = request.args.get('tipo', 1)
    return jsonify(buscar_pedido_manual(pedido, tipo))

@api_bp.route('/dados_fornecedor')
def api_dados_fornecedor():
    cod = request.args.get('cod_cli')
    if not cod: return jsonify([])
    return jsonify(buscar_contato_fornecedor(cod))

@api_bp.route('/atualizar_status', methods=['POST'])
def api_upd_status():
    """
    Rota para salvar manualmente o status de uma nota/pedido (ex: Pendente -> Concluído).
    Salva em um arquivo JSON local (simulando um banco de dados simples).
    """
    try:
        p = request.json
        chave = p['chave']   # Identificador único (Chave da Nota ou Pedido)
        novo = p['status']   # Novo status selecionado
        
        # Carrega o banco de dados local (arquivo json)
        db = _carregar_workflow_local()
        
        # Lógica para atualizar ou criar o registro se não existir
        if chave not in db or isinstance(db[chave], str): 
            db[chave] = {'status': novo}
        else: 
            db[chave]['status'] = novo
            
        # Salva de volta no arquivo
        _salvar_workflow_local(db)
        
        return jsonify({'success': True})
    except: 
        return jsonify({'success': False})