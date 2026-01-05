import os
from dotenv import load_dotenv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'vila_apps_secret_key'
    DB_DRIVER = os.environ.get('DB_DRIVER')
    DB_SERVER = os.environ.get('DB_SERVER')
    DB_DATABASE = os.environ.get('DB_DATABASE')
    DB_USER = os.environ.get('DB_USER')
    DB_PASSWORD = os.environ.get('DB_PASSWORD')
    
    # CORREÇÃO: Variável direta (sem @property e sem self)
    # O Python consegue ler as variáveis definidas acima dentro da mesma classe
    SQL_CONNECTION_STRING = (
        f'Driver={DB_DRIVER};'
        f'Server={DB_SERVER};'
        f'Database={DB_DATABASE};'
        f'UID={DB_USER};'
        f'PWD={DB_PASSWORD};'
        f'Encrypt=no;'
    )

    # Caminhos
    CAMINHO_XML_PADRAO = os.environ.get('CAMINHO_XML_PADRAO')
    PATH_CACHE = os.environ.get('PATH_CACHE')
    PASTAS_IGNORADAS = [p.strip() for p in os.environ.get('PASTAS_IGNORADAS').split(",") if p.strip()]
    CFOPS_PADRAO =  [p.strip() for p in os.environ.get('CFOPS_PADRAO').split(",") if p.strip()]