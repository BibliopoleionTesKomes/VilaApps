from flask import Flask
from config import Config

def create_app():
    """
    Função 'Fábrica de Aplicação'.
    """
    
    # 1. Instanciação do Flask
    app = Flask(__name__)
    
    # 2. Carregamento de Configurações
    app.config.from_object(Config)
    
    # 3. Importação dos Módulos (Blueprints)
    from app.blueprints.consignacoes import consignacoes_bp
    from app.blueprints.gestao import gestao_bp
    from app.blueprints.fiscal import fiscal_bp
    from app.blueprints.conferencia import conferencia_bp
    from app.blueprints.api import api_bp
    
    # 4. Registro dos Módulos (CORREÇÃO AQUI)
    
    # Antes estava assim: app.register_blueprint(consignacoes_bp)
    # Isso fazia ele roubar a página inicial.
    
    # CORREÇÃO: Adicionamos url_prefix='/consignacoes'
    app.register_blueprint(consignacoes_bp, url_prefix='/consignacoes')
    
    app.register_blueprint(gestao_bp, url_prefix='/gestao')
    app.register_blueprint(fiscal_bp, url_prefix='/fiscal')
    app.register_blueprint(conferencia_bp, url_prefix='/conferencia')
    app.register_blueprint(api_bp, url_prefix='/api')
    
    # 5. Rota da Página Inicial (Menu Principal)
    # Agora que libertámos o endereço '/', esta rota vai funcionar e abrir o Menu.
    @app.route('/')
    def menu():
        from flask import render_template
        return render_template('menu.html')
        
    return app