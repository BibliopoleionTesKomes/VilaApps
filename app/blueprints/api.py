# Importações necessárias do Flask e bibliotecas padrão
from flask import Blueprint, jsonify, request, current_app
import threading

# Importações dos repositórios (funções que buscam dados no banco ou arquivos)
# É boa prática separar a lógica de acesso a dados (repositório) da lógica da rota (controller)
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

# Criação do Blueprint 'api'
# Blueprints ajudam a organizar a aplicação em módulos.
# Tudo que for definido aqui será acessível via prefixo definido no create_app (ex: /api)
api_bp = Blueprint('api', __name__)

# --- ROTAS DE CONSULTA BÁSICA (DADOS AUXILIARES) ---

@api_bp.route('/fornecedores')
def api_fornecedores():
    """
    Rota para listar fornecedores.
    Recebe um parâmetro 'tipo' via URL (ex: ?tipo=1) para filtrar o tipo de fornecedor.
    Retorna um JSON com a lista de fornecedores.
    """
    # request.args.get pega parâmetros da URL (?chave=valor)
    tipo = request.args.get('tipo', 1) 
    dados = listar_fornecedores(tipo)
    return jsonify(dados) # jsonify converte dicionários/listas Python para JSON (formato web)

@api_bp.route('/filiais')
def api_filiais():
    """
    Rota para listar filiais de um fornecedor específico.
    Exige o parâmetro 'cod_cli' (código do cliente/fornecedor).
    """
    cod_cliente = request.args.get('cod_cli')
    tipo = request.args.get('tipo', 1)
    
    # Chama a função do repositório passando os filtros
    dados = listar_filiais_do_fornecedor(cod_cliente, tipo)
    return jsonify(dados)

@api_bp.route('/pedidos')
def api_pedidos():
    """
    Rota para listar pedidos (propostas) com base em vários filtros.
    Útil para preencher comboboxes ou listas de seleção.
    """
    # 'a' é um atalho para os argumentos da requisição
    a = request.args
    
    # Passa todos os parâmetros possíveis para a função de busca
    dados = listar_pedidos_do_fornecedor(
        a.get('cod_cli'), 
        a.get('cod_filial'), 
        a.get('data_ini'), 
        a.get('data_fim'), 
        a.get('tipo', 1)
    )
    return jsonify(dados)

@api_bp.route('/buscar_pedido')
def api_buscar_pedido():
    """
    Rota específica para buscar os ITENS de um pedido.
    Usada quando o usuário seleciona um pedido e clica em 'Confrontar'.
    """
    pedido = request.args.get('pedido')
    tipo = request.args.get('tipo', 1)
    
    # Retorna a lista de itens do pedido encontrado
    return jsonify(buscar_pedido_manual(pedido, tipo))

@api_bp.route('/dados_fornecedor')
def api_dados_fornecedor():
    """
    Rota para buscar dados de contato (email, telefone) de um fornecedor.
    Usada no modal de contato.
    """
    cod = request.args.get('cod_cli')
    
    # Se não vier código, retorna lista vazia para evitar erro
    if not cod: 
        return jsonify([])
        
    return jsonify(buscar_contato_fornecedor(cod))

# --- ROTAS DE PROCESSAMENTO (TAREFAS DEMORADAS) ---

@api_bp.route('/iniciar_processamento', methods=['POST'])
def api_iniciar():
    """
    Inicia uma tarefa pesada em segundo plano (Background Thread).
    Isso é crucial para não travar a tela do usuário enquanto o sistema processa dados.
    """
    # Verifica se já existe algo rodando para evitar conflitos
    if STATUS_GLOBAL['status'] == 'rodando':
        return jsonify({'status': 'ocupado'})
    
    # Limpa contadores anteriores
    resetar_progresso()
    
    # Pega qual módulo será processado (ex: 'acerto', 'devolucao') do corpo do POST
    modulo = request.json.get('modulo', 'geral')
    
    # PREPARAÇÃO DA THREAD:
    # O Flask 'current_app' (que guarda configs) não existe dentro de Threads novas.
    # Por isso, precisamos copiar as configurações necessárias para um dicionário simples
    # e passar esse dicionário para a função da thread.
    app_config = {
        'CAMINHO_XML_PADRAO': current_app.config['CAMINHO_XML_PADRAO'],
        'PASTAS_IGNORADAS': current_app.config['PASTAS_IGNORADAS'],
        'CFOPS_PADRAO': current_app.config['CFOPS_PADRAO']
    }
    
    # Cria e inicia a Thread
    # target: a função que vai rodar
    # args: os argumentos que essa função vai receber
    thread = threading.Thread(target=tarefa_background, args=(modulo, app_config))
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

# --- ROTAS DE ATUALIZAÇÃO (ESCRITA DE DADOS) ---

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