# --- IMPORTAÇÕES ---
import json  # Para salvar o histórico localmente
import os    # Para verificar se arquivos existem
import pandas as pd
from app.database import execute_query

# Nome do arquivo onde salvamos o histórico de mensagens/workflow.
# Isso funciona como um "banco de dados portátil" para dados que não existem no ERP.
ARQUIVO_STATUS_DB = 'vila_status_db.json'

# --- FUNÇÕES AUXILIARES (WORKFLOW LOCAL) ---

def _carregar_workflow_local():
    """
    Lê o arquivo JSON local e devolve um dicionário Python.
    Se o arquivo não existir ou estiver corrompido, devolve um dicionário vazio {}.
    """
    if os.path.exists(ARQUIVO_STATUS_DB):
        try:
            with open(ARQUIVO_STATUS_DB, 'r', encoding='utf-8') as f:
                return json.load(f)
        except: 
            return {} # Falha silenciosa: retorna vazio para não travar o sistema
    return {}

def _salvar_workflow_local(dados):
    """
    Grava o dicionário de dados no arquivo JSON local.
    Retorna True se der certo, False se der erro.
    """
    try:
        with open(ARQUIVO_STATUS_DB, 'w', encoding='utf-8') as f:
            json.dump(dados, f, indent=4) # indent=4 deixa o arquivo legível para humanos
        return True
    except Exception as e:
        print(f"Erro crítico ao salvar JSON: {e}")
        return False

# --- FUNÇÕES DE NEGÓCIO (SQL SERVER) ---

def buscar_contato_fornecedor(cod_cli):
    """
    Busca telefone e email do fornecedor no ERP.
    Usada quando o usuário clica no botão 'Contato'.
    """
    sql = "SELECT CODECLI AS COD, FANTASIA AS NOME, CGC_CPF AS CNPJ, TELEFONE, EMAIL FROM ERIS_LIVRARIAVILA.DBO.CLIENTE WHERE CODECLI = ?"
    df = execute_query(sql, [cod_cli])
    
    if not df.empty:
        # fillna('') remove "NaN" (Not a Number) feio do pandas, trocando por vazio
        df = df.fillna('')
        return df.to_dict('records')
    return []

# --- FUNÇÕES DE MANIPULAÇÃO DO HISTÓRICO ---

def adicionar_historico_repo(chave, responsavel, data, obs):
    """
    Adiciona uma nova mensagem ao histórico de uma proposta.
    Atualiza tanto a lista de histórico quanto os campos de 'última interação'.
    """
    db = _carregar_workflow_local()
    chave = str(chave) # Garante que a chave (Nº Pedido) seja string
    
    # Inicializa a estrutura se for a primeira vez
    if chave not in db: 
        db[chave] = {'historico': []}
    # Migração: Se era string antiga (apenas status), converte para estrutura nova
    elif isinstance(db[chave], str): 
        db[chave] = {'status': db[chave], 'historico': []}
    
    if 'historico' not in db[chave]: 
        db[chave]['historico'] = []
    
    # Cria o objeto da nova mensagem
    novo = {'responsavel': responsavel, 'data': data, 'obs': obs}
    
    # Adiciona à lista
    db[chave]['historico'].append(novo)
    
    # Atualiza o 'resumo' para exibição rápida na tabela
    db[chave].update({'responsavel': responsavel, 'data_contato': data, 'observacao': obs})
    
    return _salvar_workflow_local(db)

def excluir_historico_repo(chave, index):
    """
    Remove uma mensagem específica do histórico pelo índice (0, 1, 2...).
    Se apagar a última, atualiza o resumo com a penúltima mensagem.
    """
    db = _carregar_workflow_local()
    chave = str(chave)
    
    if chave in db and 'historico' in db[chave]:
        lista = db[chave]['historico']
        
        # Verifica se o índice é válido
        if 0 <= index < len(lista):
            lista.pop(index) # Remove o item
            
            # Recalcula o resumo baseado na última mensagem que sobrou
            if lista:
                ult = lista[-1]
                db[chave].update({'responsavel': ult.get('responsavel',''), 'data_contato': ult.get('data',''), 'observacao': ult.get('obs','')})
            else:
                # Se apagou tudo, limpa o resumo
                db[chave].update({'responsavel': '', 'data_contato': '', 'observacao': ''})
            
            return _salvar_workflow_local(db)
    return False

def listar_propostas_gestao(data_ini, data_fim, status_filtro=None, filtro_proposta=None, filtro_fornecedor=None, filtro_filial=None):
    """
    A QUERY MONSTRA. Busca propostas, calcula totais, pendências e faturamentos.
    Esta função alimenta o painel principal de gestão.
    """
    # OUTER APPLY: Uma técnica avançada de SQL. É como fazer um "loop" para cada linha da tabela principal (P),
    # rodando uma subconsulta que soma os itens (I) daquela proposta específica.
    # Isso permite trazer somatórios (VLR_TOTAL, QTD_ITENS) numa única consulta rápida.
    sql = """
        SELECT 
            P.PEDIDO, P.DT_PED, C_FILIAL.FANTASIA AS FILIAL, C_FORN.FANTASIA AS FORNECEDOR,
            C_FORN.CODECLI AS COD_FORNECEDOR, P.STATUS AS COD_STATUS,
            CASE 
                WHEN P.STATUS = 1 THEN 'ENVIADO'
                WHEN P.STATUS = 2 THEN 'CONCLUÍDO'
                WHEN P.STATUS = 3 THEN 'DIGITAÇÃO'
                ELSE 'OUTROS'
            END AS STATUS_DESC,
            ISNULL(ITENS.VLR_TOTAL, 0) AS VALOR_TOTAL,
            ISNULL(ITENS.QTD_ITENS, 0) AS QTD_ITENS,
            ISNULL(ITENS.QTD_FATURADA_SUM, 0) AS QTD_FATURADA,
            ISNULL(ITENS.QTD_PENDENTE_SUM, 0) AS QTD_PENDENTE,
            ISNULL(ITENS.VLR_PENDENTE_SUM, 0) AS VALOR_PENDENTE
        FROM ERIS_LIVRARIAVILA.DBO.PEDC_CAB P
        INNER JOIN ERIS_LIVRARIAVILA.DBO.PEDC_CAB_CONSIG PC ON P.PEDIDO = PC.PEDIDO
        LEFT JOIN ERIS_LIVRARIAVILA.DBO.CLIENTE C_FILIAL ON P.EMITENTE = C_FILIAL.CODECLI
        LEFT JOIN ERIS_LIVRARIAVILA.DBO.CLIENTE C_FORN ON P.CODECLI = C_FORN.CODECLI
        OUTER APPLY (
            SELECT SUM(I.QTT * I.PRECUNITLIQ) AS VLR_TOTAL, COUNT(*) AS QTD_ITENS,
            SUM(ISNULL(I.QTT_FATURADO, 0)) AS QTD_FATURADA_SUM,
            SUM(CASE WHEN I.STATUS <> 5 THEN (I.QTT - ISNULL(I.QTT_FATURADO, 0)) ELSE 0 END) AS QTD_PENDENTE_SUM,
            SUM(CASE WHEN I.STATUS <> 5 THEN (I.QTT - ISNULL(I.QTT_FATURADO, 0)) * I.PRECUNITLIQ ELSE 0 END) AS VLR_PENDENTE_SUM
            FROM ERIS_LIVRARIAVILA.DBO.PEDC_ITEM I WHERE I.PEDIDO = P.PEDIDO
        ) ITENS
        WHERE PC.TIPO_ACERTO = 1 AND P.DT_PED >= ? AND P.DT_PED <= ?
    """
    
    # Construção Dinâmica dos Filtros SQL
    params = [data_ini, data_fim]
    if status_filtro and status_filtro != 'todos': 
        sql += " AND P.STATUS = ?"
        params.append(status_filtro)
    if filtro_proposta: 
        sql += " AND CAST(P.PEDIDO AS VARCHAR) LIKE ?"
        params.append(f"%{filtro_proposta}%")
    if filtro_fornecedor: 
        sql += " AND C_FORN.FANTASIA LIKE ?"
        params.append(f"%{filtro_fornecedor}%")
    if filtro_filial: 
        sql += " AND C_FILIAL.FANTASIA LIKE ?"
        params.append(f"%{filtro_filial}%")
        
    sql += " ORDER BY P.DT_PED DESC, P.PEDIDO DESC"

    df = execute_query(sql, params)
    
    # Pós-processamento dos dados (Data Enrichment)
    if not df.empty:
        # Carrega dados locais (JSON) para misturar com os dados do SQL
        workflow_db = _carregar_workflow_local()
        
        # Função interna para buscar o último status de workflow de cada linha
        def get_wf(row):
            ped = str(row['PEDIDO'])
            d = workflow_db.get(ped, {})
            # Tratamento de legado
            if isinstance(d, str): d = {'status': d}
            
            hist = d.get('historico', [])
            ult = hist[-1] if hist else {}
            
            # Retorna uma série para ser incorporada como colunas no DataFrame principal
            return pd.Series({
                'WF_RESPONSAVEL': ult.get('responsavel', d.get('responsavel', '')),
                'WF_DATA_COBRANCA': ult.get('data', d.get('data_contato', '')),
                'WF_OBS': ult.get('obs', d.get('observacao', '')),
                'WF_HISTORY_JSON': json.dumps(hist) if hist else '[]' # JSON stringificado para o front-end
            })
        
        # Aplica a função linha a linha
        df = pd.concat([df, df.apply(get_wf, axis=1)], axis=1)
        
        try:
            # Formatação de Datas e Moedas para exibição bonita
            df['DT_PED'] = pd.to_datetime(df['DT_PED'])
            df['DT_PED_FMT'] = df['DT_PED'].dt.strftime('%d/%m/%Y')
            
            def fmt(x): return f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            
            df['VALOR_TOTAL_FMT'] = df['VALOR_TOTAL'].fillna(0).apply(fmt)
            df['VALOR_PENDENTE'] = df['VALOR_PENDENTE'].fillna(0).apply(fmt)
            
            return df.to_dict('records')
        except: 
            return []
            
    return []

def buscar_detalhes_proposta(pedido):
    """
    Busca os ITENS detalhados de uma proposta para o modal de detalhes.
    Calcula o status item a item (Pendente, Faturado, Cancelado).
    """
    sql = """
        SELECT I.PRODCODE AS CODIGO, P.DESCRICAO, P.COD_BARRA AS ISBN, I.QTT AS QUANTIDADE,
        ISNULL(I.QTT_FATURADO, 0) AS QTD_FATURADA, (I.QTT - ISNULL(I.QTT_FATURADO, 0)) AS QTD_PENDENTE,
        I.PRECUNITLIQ AS VL_UNIT, (I.QTT * I.PRECUNITLIQ) AS VL_TOTAL,
        CASE 
            WHEN I.STATUS=2 THEN 'PENDENTE' 
            WHEN I.STATUS=4 THEN 'FATURADO' 
            WHEN I.STATUS=5 THEN 'CANCELADO' 
            ELSE CAST(I.STATUS AS VARCHAR) 
        END AS STATUS_ITEM
        FROM ERIS_LIVRARIAVILA.DBO.PEDC_ITEM I 
        LEFT JOIN ERIS_LIVRARIAVILA.DBO.PRODUTO P ON I.PRODCODE = P.PRODCODE
        WHERE I.PEDIDO = ? ORDER BY P.DESCRICAO
    """
    df = execute_query(sql, [pedido])
    
    if not df.empty:
        try:
            # Formatação de valores monetários
            df['VL_UNIT'] = df['VL_UNIT'].fillna(0).apply(lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
            df['VL_TOTAL'] = df['VL_TOTAL'].fillna(0).apply(lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
            
            # Garante que quantidades sejam inteiros (sem casas decimais 1.0 -> 1)
            df['QTD_FATURADA'] = df['QTD_FATURADA'].astype(int)
            df['QTD_PENDENTE'] = df['QTD_PENDENTE'].astype(int)
            
            return df.to_dict('records')
        except: 
            return []
    return []