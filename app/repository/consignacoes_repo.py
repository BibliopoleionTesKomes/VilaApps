# --- IMPORTAÇÕES ---
# Importa a nossa ferramenta personalizada para rodar SQL.
# Ela já cuida de abrir a conexão, rodar o comando e fechar a conexão.
from app.database import execute_query

def buscar_dados_consignacoes():
    """
    Busca no banco de dados todas as consignações ativas.
    Retorna um DataFrame do Pandas com os dados para o relatório.
    """
    
    # A query SQL seleciona colunas específicas de várias tabelas ligadas (JOINs).
    # Explicação de campos calculados:
    # 1. FORMAT(..., 'dd'): Pega apenas o dia do vencimento (ex: 05, 10, 15).
    # 2. CNPJ Formatado: A lógica com REPLICATE/RIGHT garante que o CNPJ tenha 14 dígitos com zeros à esquerda.
    sql = """
        SELECT  
            F.CODECLI AS CodFornecedor,
            F.Fantasia AS Nome_Fantasia, 
            Loja.FANTASIA AS Filial,  
            co.DESC_NOME_CONSIGNACAO AS Controle,
            FORMAT(co.DATA_VENCIMENTO, 'dd') AS 'Dia de acerto',
            co.DESC_OBS AS Obs,
            -- Formatação manual de CNPJ para garantir 14 dígitos no Excel
            F.CGC_CPF AS CNPJ,
            F.RAZAOSOCIAL AS 'Razao social',    
            condpag_desc AS 'Prazo',
            co.VALR_ALIQUOTA_DESCONTO AS Desconto,
            co.DATA_VENCIMENTO,  
            co.DATA_INICIO
            
        FROM consignacoes co
        -- Liga a consignação ao cadastro do Fornecedor
        INNER JOIN CLIENTE F ON F.CODECLI = co.CODG_FORNECEDOR
        -- Liga a consignação ao cadastro da Loja (Filial)
        INNER JOIN CLIENTE Loja ON Loja.CODECLI = co.FILIAL  
        -- Liga para pegar a descrição do prazo de pagamento (ex: 30/60 dias)
        INNER JOIN COND_PAG c ON c.CONDPAG_ID = co.CONDPAG_ID
        
        -- Filtro Importante: Apenas consignações Ativas ('A')
        WHERE co.INDI_STATUS = 'A'
        
        -- Ordena pelo primeiro campo (CodFornecedor)
        ORDER BY 1;
    """
    
    # Executa a consulta.
    # O retorno é um DataFrame do Pandas, que é como uma planilha do Excel na memória.
    # Se der erro, o 'execute_query' lá no app/database.py vai tratar e mostrar no console.
    return execute_query(sql)