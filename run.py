from app import create_app
from dotenv import load_dotenv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

app = create_app()

if __name__ == '__main__':
    app.run(debug=True, port=5002)