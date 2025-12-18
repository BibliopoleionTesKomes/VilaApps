# --- IMPORTAÇÕES ---
# Flask: Framework web principal.
# Blueprint: Ferramenta para organizar o projeto em "fatias" ou módulos independentes.
# render_template: Função que pega um arquivo HTML na pasta 'templates' e injeta dados Python nele.
from flask import Blueprint, render_template

# Importação da Camada de Dados (Repositório)
# É boa prática não escrever SQL aqui. Importamos a função que já sabe como falar com o banco.
from app.repository.consignacoes_repo import buscar_dados_consignacoes

# Criação do Blueprint
# Definimos um nome interno ('consignacoes') para este grupo de rotas.
consignacoes_bp = Blueprint('consignacoes', __name__)

# --- ROTAS (ENDPOINTS) ---

@consignacoes_bp.route('/')
def index():
    """
    Rota principal para listar as Consignações.
    URL esperada: /consignacoes/ (definido no prefixo do create_app)
    """
    try:
        # 1. Chama a camada de dados
        # A função retorna um DataFrame do Pandas com os dados brutos do SQL.
        df = buscar_dados_consignacoes()
        
        # 2. Conversão de Dados
        # O template HTML (Jinja2) trabalha melhor com listas de dicionários do que com DataFrames puros.
        # .to_dict('records') transforma a tabela em: [{'coluna': 'valor'}, {'coluna': 'valor'}...]
        results = df.to_dict('records')
        
        # Inicializa mensagem de erro como vazia (sucesso)
        erro_msg = None
        
    except Exception as e:
        # 3. Tratamento de Erros (Try/Except)
        # Se o banco cair ou houver erro no SQL, o site não "quebra" na cara do usuário.
        # Capturamos o erro, logamos no terminal e passamos uma mensagem segura para a tela.
        print(f"CRITICAL ERROR - Rota Consignações: {e}")
        erro_msg = f"Erro ao processar dados: {e}"
        results = [] # Lista vazia para garantir que o HTML renderize sem dados, mas sem erro.

    # 4. Renderização (Resposta)
    # Entrega o arquivo HTML preenchido com as variáveis 'dados' e 'erro'.
    return render_template('consignacoes_lista.html', dados=results, erro=erro_msg)