import os

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'vila_apps_secret_key'
    
    # Configurações do Banco de Dados
    DB_SERVER = 'servererpsql.livrariadavila.com.br'
    DB_DATABASE = 'ERIS_LIVRARIAVILA'
    DB_USER = 'Leandro'
    DB_PASS = 'Leandro@123'
    
    # CORREÇÃO: Variável direta (sem @property e sem self)
    # O Python consegue ler as variáveis definidas acima dentro da mesma classe
    SQL_CONNECTION_STRING = (
        f'Driver={{ODBC Driver 11 for SQL Server}};'
        f'Server={DB_SERVER};'
        f'Database={DB_DATABASE};'
        f'UID={DB_USER};'
        f'PWD={DB_PASS};'
        f'Encrypt=no;'
    )

    # Caminhos
    CAMINHO_XML_PADRAO = r"C:\Users\AS informática\Documents\Modelos Python\Confronto _ NFE\xml teste"
    PASTAS_IGNORADAS = ['enviados', 'associado', 'associados', 'canceladas', 'inutilizadas']
    CFOPS_PADRAO = ['5113', '5114', '6113', '6114', '1113', '1114', '2113', '2114']