# --- IMPORTAÇÕES ---
import pandas as pd
import numpy as np
import re
import uuid
import time
import os
import json
import tempfile
from io import BytesIO, StringIO
from app.repository.conferencia_repo import buscar_acerto_sql_repo, buscar_vendas_sql_repo

# --- CONFIGURAÇÃO DE PERSISTÊNCIA (DISCO EM VEZ DE RAM) ---
# Usamos a pasta temporária do sistema para salvar os dados da conferência.
TEMP_DIR = tempfile.gettempdir()

def _get_file_path(did):
    """Gera o caminho do arquivo baseado no ID da sessão."""
    return os.path.join(TEMP_DIR, f'vila_conf_{did}.json')

# --- FUNÇÕES DE LIMPEZA ---
def _limpar_isbn(isbn_sujo):
    if pd.isna(isbn_sujo): return None
    isbn_str = str(isbn_sujo).replace('.0', '').strip()
    isbn_limpo = re.sub(r'[^0-9]', '', isbn_str)
    return isbn_limpo if len(isbn_limpo) >= 8 else None

def _normalizar_nome_filial(nome_sujo):
    if pd.isna(nome_sujo): return "desconhecida"
    return str(nome_sujo).strip().lower()

def _garantir_dataframe_seguro(df, colunas_obrigatorias):
    if df is None or df.empty: return pd.DataFrame(columns=colunas_obrigatorias)
    for col in colunas_obrigatorias:
        if col not in df.columns:
            df[col] = 0 if any(k in col for k in ['Quant', 'Valor', 'Vl.', 'Desconto']) else ''
    return df

# --- PROCESSAMENTO SQL ---
def processar_acerto_sql_service(pedidos_list):
    df_raw = buscar_acerto_sql_repo(pedidos_list)
    cols_retorno = ['filial', 'ISBN', 'Titulo', 'Quant', 'Vl. Unit._acerto', 'Desconto', 'fornecedor']
    if df_raw.empty: return pd.DataFrame(columns=cols_retorno), "Sem Filial", "Sem Fornecedor"
    
    df_raw.columns = [str(c).upper().strip() for c in df_raw.columns]
    df = df_raw.copy()
    for col, default in [('FILIAL', 'desconhecida'), ('ISBN', ''), ('TITULO', 'Item Sem Nome')]:
        if col not in df.columns: df[col] = default
        
    df['filial'] = df['FILIAL'].apply(_normalizar_nome_filial)
    df['ISBN_limpo'] = df['ISBN'].apply(_limpar_isbn)
    df = df[df['ISBN_limpo'].notna()].copy()
    
    col_qtd = next((c for c in ['QUANT', 'QTT'] if c in df.columns), None)
    df['Quant'] = pd.to_numeric(df.get(col_qtd, 0), errors='coerce').fillna(0)
    
    col_vlu = next((c for c in ['VLUNIT', 'VL_UNIT', 'PRECUNITTAB'] if c in df.columns), 'VlUnit')
    df['VlUnit'] = pd.to_numeric(df.get(col_vlu, 0), errors='coerce').fillna(0)
    
    col_liq = next((c for c in ['VLLIQITEM', 'VLLIQU', 'PRECUNITLIQ'] if c in df.columns), 'VlLiqItem')
    df['VlLiqItem'] = pd.to_numeric(df.get(col_liq, df['VlUnit']), errors='coerce').fillna(0)

    df['DescontoCalculado'] = df.apply(lambda row: (1 - (row['VlLiqItem'] / row['VlUnit'])) if row['VlUnit'] > 0 else 0, axis=1)
    
    fornecedor_global = df.get('FORNECEDOR', pd.Series(["Indefinido"])).iloc[0]

    df_acerto = df.groupby(['ISBN_limpo', 'filial'], as_index=False).agg(
        Quant=('Quant', 'sum'),
        Titulo=('TITULO', 'first'),
        VlUnit=('VlUnit', 'first'),
        DescontoCalculado=('DescontoCalculado', 'first')
    )
    df_acerto['fornecedor'] = fornecedor_global
    df_acerto = df_acerto.rename(columns={'ISBN_limpo': 'ISBN', 'VlUnit': 'Vl. Unit._acerto', 'DescontoCalculado': 'Desconto'})
    return _garantir_dataframe_seguro(df_acerto, cols_retorno), "Múltiplas", fornecedor_global

def processar_vendas_sql_service(data_ini, data_fim, fornecedor_id):
    cols_padrao = ['filial', 'ISBN', 'Quant_venda', 'Vl. Unit._venda', 'Preco_Venda_F']
    df_raw = buscar_vendas_sql_repo(data_ini, data_fim, fornecedor_id)
    if df_raw.empty: return pd.DataFrame(columns=cols_padrao)
    
    df_raw.columns = [str(c).upper().strip() for c in df_raw.columns]
    df = df_raw.copy()
    for col, default in [('FILIAL', 'desconhecida'), ('ISBN', '')]:
        if col not in df.columns: df[col] = default
        
    df['filial'] = df['FILIAL'].apply(_normalizar_nome_filial)
    df['ISBN'] = df['ISBN'].apply(_limpar_isbn)
    df = df.dropna(subset=['ISBN']).copy()
    
    col_qtd = next((c for c in ['QUANTIDADE', 'QTT', 'QUANT'] if c in df.columns), 'Quantidade')
    col_total = next((c for c in ['VALOR_TOTAL', 'VALOR'] if c in df.columns), 'Valor_Total')
    
    df['Quantidade'] = pd.to_numeric(df.get(col_qtd, 0), errors='coerce').fillna(0)
    df['Valor_Total'] = pd.to_numeric(df.get(col_total, 0), errors='coerce').fillna(0)
    
    df_venda = df.groupby(['filial', 'ISBN'], as_index=False).agg(
        Quant_venda=('Quantidade', 'sum'), 
        Vl_Unit__venda=('Valor_Total', 'sum')
    )
    df_venda = df_venda.rename(columns={'Vl_Unit__venda': 'Vl. Unit._venda'})
    return _garantir_dataframe_seguro(df_venda, cols_padrao)

# --- PROCESSAMENTO EXCEL ---
def carregar_acerto_excel(stream):
    try:
        dados_sem_cabecalho = pd.read_excel(stream, header=None)
        if dados_sem_cabecalho.empty: return pd.DataFrame(), "", ""

        filial_bruta = str(dados_sem_cabecalho.iloc[0, 2])
        fornecedor = str(dados_sem_cabecalho.iloc[15, 1])
        filial = _normalizar_nome_filial(filial_bruta)
        
        header_row_index = -1
        nomes_colunas = []
        for i, row in dados_sem_cabecalho.iterrows():
            vals = [str(x).strip() for x in row.values]
            if 'ISBN' in vals and ('Quant' in vals or 'Desc.' in vals):
                header_row_index = i; nomes_colunas = vals; break
        
        if header_row_index == -1: return pd.DataFrame(), "", ""
        
        acerto = pd.DataFrame(dados_sem_cabecalho.iloc[header_row_index + 1:].values, columns=nomes_colunas)
        
        col_map = {
            next((c for c in nomes_colunas if 'Titulo' in c), 'Titulo'): 'Titulo',
            next((c for c in nomes_colunas if 'ISBN' in c), 'ISBN'): 'ISBN',
            next((c for c in nomes_colunas if 'Quant' in c), 'Quant'): 'Quant',
            next((c for c in nomes_colunas if 'Vl. Unit.' in c), 'Vl. Unit.'): 'Vl. Unit.',
            next((c for c in nomes_colunas if 'Desc.' in c), 'Desconto'): 'Desconto'
        }
        acerto = acerto.rename(columns=col_map)
        acerto['ISBN_limpo'] = acerto['ISBN'].apply(_limpar_isbn)
        acerto = acerto[acerto['ISBN_limpo'].notna()].copy()
        acerto['filial'] = filial
        
        for c in ['Desconto', 'Quant', 'Vl. Unit.']: acerto[c] = pd.to_numeric(acerto[c], errors='coerce').fillna(0)
        acerto['Desconto'] = acerto['Desconto'].apply(lambda x: x/100.0 if x > 1 else x)
        
        df_acerto = acerto.groupby(['ISBN_limpo', 'filial'], as_index=False).agg(
            Quant=('Quant', 'sum'), Titulo=('Titulo', 'first'), Vl_Unit__acerto=('Vl. Unit.', 'first'), Desconto=('Desconto', 'first')
        )
        df_acerto = df_acerto.rename(columns={'ISBN_limpo': 'ISBN', 'Vl_Unit__acerto': 'Vl. Unit._acerto'})
        return _garantir_dataframe_seguro(df_acerto, ['filial','ISBN','Quant']), filial_bruta, fornecedor
    except: return pd.DataFrame(), "Erro", "Erro"

def carregar_venda_excel(stream, col_qtd_nome='Quant_venda'):
    try:
        venda = pd.read_excel(stream, header=None, skiprows=15)
        cols_padrao = ['filial', 'ISBN', col_qtd_nome]
        if venda.empty: return pd.DataFrame(columns=cols_padrao)

        colunas_para_ler = {0: 'filial', 2: 'ISBN', 5: 'Preco_Venda_F', 6: 'Vl. Unit._venda_bruto', 7: col_qtd_nome}
        colunas_existentes = {c: n for c, n in colunas_para_ler.items() if c in venda.columns}
        venda = venda[colunas_existentes.keys()].rename(columns=colunas_existentes)
        
        venda['ISBN'] = venda['ISBN'].apply(_limpar_isbn)
        venda['filial'] = venda['filial'].apply(_normalizar_nome_filial)
        venda[col_qtd_nome] = pd.to_numeric(venda[col_qtd_nome], errors='coerce').fillna(0)
        venda = venda.dropna(subset=['ISBN', col_qtd_nome]).copy()
        
        agg_dict = {col_qtd_nome: 'sum'}
        if 'Vl. Unit._venda_bruto' in venda.columns:
            venda['Vl. Unit._venda_bruto'] = pd.to_numeric(venda['Vl. Unit._venda_bruto'], errors='coerce').fillna(0)
            agg_dict['Vl. Unit._venda_bruto'] = 'sum'

        df_venda = venda.groupby(['filial', 'ISBN'], as_index=False).agg(agg_dict)
        if 'Vl. Unit._venda_bruto' in df_venda.columns:
            df_venda = df_venda.rename(columns={'Vl. Unit._venda_bruto': 'Vl. Unit._venda'})
        return _garantir_dataframe_seguro(df_venda, cols_padrao)
    except: return pd.DataFrame(columns=['filial', 'ISBN', col_qtd_nome])

def carregar_quebra_inventario(stream):
    """
    Carrega o relatório de quebra.
    Mapeamento corrigido: Coluna H (7) para ISBN, J (9) para Contado, K (10) para Estoque.
    """
    try:
        stream.seek(0)
        try: df = pd.read_excel(stream, header=None)
        except: 
            stream.seek(0)
            df = pd.read_csv(stream, header=None, sep=',', encoding='latin1')
            
        if df.empty: return pd.DataFrame(columns=['filial', 'ISBN', 'Quebra_Inv'])

        # Filial na linha 0, coluna 4 (E)
        filial = _normalizar_nome_filial(str(df.iloc[0, 4]).strip())
        
        # Procura a linha de cabeçalho (procura 'ISBN' em qualquer coluna)
        idx_header = -1
        for i, row in df.iterrows():
            row_text = [str(val).strip().upper() for val in row.values]
            if 'ISBN' in row_text:
                idx_header = i
                break
                
        if idx_header == -1: 
            return pd.DataFrame(columns=['filial', 'ISBN', 'Quebra_Inv'])
        
        # Dados começam após o cabeçalho
        df_d = df.iloc[idx_header+1:].copy()
        
        # Mapeamento para as colunas H(7), J(9) e K(10)
        df_d = df_d.rename(columns={7: 'ISBN', 9: 'Contado', 10: 'Estoque'})
        
        df_d['ISBN'] = df_d['ISBN'].apply(_limpar_isbn)
        df_d = df_d.dropna(subset=['ISBN'])
        
        # Converte para numérico com segurança
        df_d['Estoque'] = pd.to_numeric(df_d['Estoque'], errors='coerce').fillna(0)
        df_d['Contado'] = pd.to_numeric(df_d['Contado'], errors='coerce').fillna(0)
        
        # Quebra = Sistema - Contagem física
        df_d['Quebra_Inv'] = df_d['Estoque'] - df_d['Contado']
        df_d['filial'] = filial
        
        res = df_d.groupby(['filial', 'ISBN'], as_index=False).agg({'Quebra_Inv': 'sum'})
        return _garantir_dataframe_seguro(res, ['filial', 'ISBN', 'Quebra_Inv'])
    except Exception as e:
        print(f"Erro no processamento da quebra: {e}")
        return pd.DataFrame(columns=['filial', 'ISBN', 'Quebra_Inv'])

# --- CÁLCULO PRINCIPAL ---

def calcular_conferencia_padrao(df_acerto, df_venda, df_acao, isbns_promo, df_quebra):
    if df_acerto.empty: return pd.DataFrame()
    
    df_acerto = _garantir_dataframe_seguro(df_acerto, ['filial', 'ISBN', 'Titulo'])
    df_venda = _garantir_dataframe_seguro(df_venda, ['filial', 'ISBN'])
    df_acao = _garantir_dataframe_seguro(df_acao, ['filial', 'ISBN'])
    df_quebra = _garantir_dataframe_seguro(df_quebra, ['filial', 'ISBN'])

    for df in [df_acerto, df_venda, df_acao, df_quebra]:
        df[['ISBN', 'filial']] = df[['ISBN', 'filial']].astype(str)

    df = pd.merge(df_acerto, df_venda, on=['filial', 'ISBN'], how='left')
    df = pd.merge(df, df_acao, on=['filial', 'ISBN'], how='left')
    df = pd.merge(df, df_quebra, on=['filial', 'ISBN'], how='left')

    cols = ['Quant', 'Quant_venda', 'Quant_acao', 'Quebra_Inv', 'Vl. Unit._acerto', 'Desconto', 'Vl. Unit._venda']
    for c in cols:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
        else: df[c] = 0
    
    if isbns_promo:
        promo_set = {_limpar_isbn(i) for i in re.split(r'[\s,;\n]+', isbns_promo) if i.strip()}
        df['Item Promocional'] = np.where(df['ISBN'].isin(promo_set) & (df['Quant_acao'] > 0), 'Sim', 'Não')
    else: df['Item Promocional'] = 'Não'

    df['Divergência Qtd.'] = (df['Quant'] - df['Quant_venda']).clip(lower=0) 
    df['Situação Qtd.'] = np.where(df['Divergência Qtd.'] > 0, 'Divergência', 'OK')
    
    mask_promo_ok = (df['Item Promocional'] == 'Sim') & (df['Situação Qtd.'] == 'OK')
    df.loc[mask_promo_ok, 'Situação Qtd.'] = 'Ação'
    
    df['Divergência Preço'] = df['Vl. Unit._acerto'] - df['Vl. Unit._venda']
    df['Situação Preço'] = np.where(abs(df['Divergência Preço']) > 0.01, 'Divergência', 'OK')
    df['Vl. Unit. Liq. Acerto'] = df['Vl. Unit._acerto'] * (1 - df['Desconto'])
    df['Vlr. Liq. Qtd. Divergência'] = df['Divergência Qtd.'] * df['Vl. Unit. Liq. Acerto']

    if 'Quebra_Inv' in df.columns:
        df['Calc_V'] = df[['Quebra_Inv', 'Quant']].min(axis=1).clip(lower=0)
        df['Quebra_Inv'] = np.where(df['Divergência Qtd.'] > 0, df['Calc_V'], 0).astype(int)
        df['Vlr. Quebra Liquida'] = df['Quebra_Inv'] * df['Vl. Unit. Liq. Acerto']
        df['Vlr. Quebra Bruta'] = df['Quebra_Inv'] * df['Vl. Unit._acerto']
    else:
        df['Quebra_Inv'] = 0; df['Vlr. Quebra Liquida'] = 0; df['Vlr. Quebra Bruta'] = 0

    # Inicializa com 0 para forçar o preenchimento manual do usuário
    df['Qtd. a Acertar'] = 0
    
    df['Vlr. Liq. A Acertar'] = 0.0
    df['Qtd. Final'] = 0.0
    df['Titulo'] = df['Titulo'].fillna('Não Informado').astype(str)
    
    return df

def gerar_planilha_acao(df_acerto, df_acao, isbns_promo):
    if df_acerto.empty: return pd.DataFrame()
    
    df_acerto = _garantir_dataframe_seguro(df_acerto, ['filial', 'ISBN', 'Titulo'])
    df_acao = _garantir_dataframe_seguro(df_acao, ['filial', 'ISBN'])
    for df in [df_acerto, df_acao]:
        df[['ISBN', 'filial']] = df[['ISBN', 'filial']].astype(str)

    df = pd.merge(df_acerto, df_acao, on=['filial', 'ISBN'], how='left')
    for c in ['Quant', 'Quant_acao', 'Vl. Unit._acerto', 'Desconto']:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
        else: df[c] = 0

    if isbns_promo:
        promo_set = {_limpar_isbn(i) for i in re.split(r'[\s,;\n]+', isbns_promo) if i.strip()}
        df['Item Promocional'] = np.where(df['ISBN'].isin(promo_set), 'Sim', 'Não')
    else: df['Item Promocional'] = 'Não'

    df = df[df['Item Promocional'] == 'Sim'].copy()

    df['Divergência Qtd.'] = (df['Quant'] - df['Quant_acao']).clip(lower=0)
    df['Situação Qtd.'] = np.where(df['Divergência Qtd.'] > 0, 'Divergência', 'OK')
    df['Vl. Unit. Liq. Acerto'] = df['Vl. Unit._acerto'] * (1 - df['Desconto'])
    df['Vlr. Liq. Qtd. Divergência'] = df['Divergência Qtd.'] * df['Vl. Unit. Liq. Acerto']

    cols_extra = ['Preco_Venda_F', 'Quant_venda', 'Quebra_Inv', 'Vlr. Quebra Liquida', 'Vlr. Quebra Bruta', 'Divergência Preço', 'Situação Preço']
    for c in cols_extra: df[c] = 0
    
    # Inicializa com 0 (Zero) para que o usuário analise e preencha
    df['Qtd. a Acertar'] = 0
    
    df['Vlr. Liq. A Acertar'] = 0.0
    df['Qtd. Final'] = 0.0
    df['Titulo'] = df['Titulo'].fillna('Não Informado').astype(str)
    return df

# --- CÁLCULOS FINAIS ---

def calcular_qtd_final(df):
    df_c = df.copy()
    cols_num = ['Quant', 'Quant_venda', 'Quant_acao', 'Qtd. a Acertar', 'Vl. Unit._acerto', 'Desconto', 'Divergência Qtd.']
    for c in cols_num: df_c[c] = pd.to_numeric(df_c.get(c, 0), errors='coerce').fillna(0)
    
    mask_div = df_c['Situação Qtd.'].eq('Divergência')
    mask_acao_edit = df_c['Item Promocional'].eq('Sim')
    mask_ok = (~mask_div) & (~mask_acao_edit)

    df_c.loc[mask_div, 'Qtd. a Acertar'] = df_c.loc[mask_div, 'Qtd. a Acertar'].clip(upper=df_c.loc[mask_div, 'Divergência Qtd.'])
    lim_acao = (df_c.loc[mask_acao_edit, 'Quant'] - df_c.loc[mask_acao_edit, 'Quant_acao']).clip(lower=0)
    df_c.loc[mask_acao_edit, 'Qtd. a Acertar'] = df_c.loc[mask_acao_edit, 'Qtd. a Acertar'].clip(upper=lim_acao)

    df_c.loc[mask_div, 'Qtd. Final'] = df_c['Quant_venda'] + df_c['Qtd. a Acertar']
    df_c.loc[mask_acao_edit, 'Qtd. Final'] = df_c['Quant_acao'] + df_c['Qtd. a Acertar']
    df_c.loc[mask_ok, 'Qtd. Final'] = df_c['Quant']
    
    df_c['Qtd. Final'] = df_c['Qtd. Final'].clip(lower=0).astype(int)
    df_c['Vlr. Liq. A Acertar'] = df_c['Qtd. a Acertar'] * df_c['Vl. Unit._acerto'] * (1 - df_c['Desconto'])
    return df_c

def calcular_qtd_final_acao(df):
    """
    CORREÇÃO CRÍTICA:
    Removi a trava .clip() que forçava a quantidade digitada a ser menor que a divergência.
    Isso permite que o usuário digite um valor para faturar mesmo que o sistema calcule divergência zero.
    """
    df_c = df.copy()
    cols_num = ['Quant', 'Quant_acao', 'Qtd. a Acertar', 'Vl. Unit._acerto', 'Desconto', 'Divergência Qtd.']
    for c in cols_num: df_c[c] = pd.to_numeric(df_c.get(c, 0), errors='coerce').fillna(0)
    
    # --- TRAVA REMOVIDA AQUI ---
    # Antes: df_c['Qtd. a Acertar'] = df_c['Qtd. a Acertar'].clip(upper=df_c['Divergência Qtd.'])
    # Agora: O valor digitado pelo usuário é respeitado incondicionalmente.
    
    mask_div = df_c['Divergência Qtd.'] > 0
    
    # Se houver divergência, soma venda + acerto manual. 
    # Se não houver divergência (ex: Ação), soma venda + acerto manual também.
    df_c['Qtd. Final'] = df_c['Quant_acao'] + df_c['Qtd. a Acertar']
    
    # Garante que não excede o total enviado (opcional, mas seguro)
    # Se quiser liberar total, remova a linha abaixo. Por segurança, mantemos não estourar o acerto original.
    df_c['Qtd. Final'] = df_c[['Qtd. Final', 'Quant']].min(axis=1)
    
    df_c['Qtd. Final'] = df_c['Qtd. Final'].clip(lower=0).astype(int)
    df_c['Vlr. Liq. A Acertar'] = df_c['Qtd. a Acertar'] * df_c['Vl. Unit._acerto'] * (1 - df_c['Desconto'])
    return df_c

def gerar_resumo_consolidado(df, sum_venda):
    df = df.copy()
    df['Vl. Unit. Liq. Acerto'] = df['Vl. Unit._acerto'] * (1 - df['Desconto'])
    df['Vlr. Liq. A Acertar'] = df['Qtd. a Acertar'] * df['Vl. Unit. Liq. Acerto']
    df['Valor Bruto (Total Acertado)'] = df['Qtd. Final'] * df['Vl. Unit._acerto']
    df['Valor Líquido (Total Acertado)'] = df['Qtd. Final'] * df['Vl. Unit. Liq. Acerto']

    res = df.groupby('filial', as_index=False).agg({
        'Valor Líquido (Total Acertado)': 'sum',
        'Valor Bruto (Total Acertado)': 'sum',
        'Vlr. Liq. Qtd. Divergência': 'sum',
        'Vlr. Liq. A Acertar': 'sum'
    })
    
    if sum_venda is not None and not sum_venda.empty:
        res = pd.merge(res, sum_venda, on='filial', how='outer').fillna(0)
    else: res['Venda Bruta'] = 0.0

    res = res.rename(columns={'Vlr. Liq. Qtd. Divergência': 'Vlr. Divergente no Acerto (Total)', 'Vlr. Liq. A Acertar': 'Vlr. Líquido a Acertar (Manual)'})
    
    totais = {
        'TOTAL ESTIMADO DA DIVERGÊNCIA LÍQUIDA': df['Vlr. Liq. Qtd. Divergência'].sum(),
        'TOTAL LÍQUIDO A ACERTAR (MANUAL)': df['Vlr. Liq. A Acertar'].sum()
    }
    return res, totais

def gerar_resumo_acao(df, sum_venda):
    return gerar_resumo_consolidado(df, sum_venda)

# --- CACHE ---
def cache_save(df, fornecedor, venda_sum, has_quebra):
    did = str(uuid.uuid4())
    
    dados_para_salvar = {
        'df_json': df.to_json(orient='records'),
        'fornecedor': fornecedor,
        'timestamp': time.time(),
        'venda_summary_json': venda_sum.to_json(orient='records') if not venda_sum.empty else '[]',
        'has_quebra': has_quebra
    }
    
    arquivo = _get_file_path(did)
    try:
        with open(arquivo, 'w', encoding='utf-8') as f:
            json.dump(dados_para_salvar, f)
    except Exception as e:
        print(f"Erro ao salvar cache em disco: {e}")
        return None

    return did

def cache_get(did):
    arquivo = _get_file_path(did)
    
    if not os.path.exists(arquivo):
        return None, None, None, False
        
    try:
        with open(arquivo, 'r', encoding='utf-8') as f:
            cached = json.load(f)
            
        df = pd.read_json(StringIO(cached['df_json']), orient='records')
        v_json = cached['venda_summary_json']
        venda_sum = pd.read_json(StringIO(v_json), orient='records') if v_json != '[]' else pd.DataFrame()
        
        return df, cached['fornecedor'], venda_sum, cached.get('has_quebra', False)
        
    except Exception as e:
        print(f"Erro ao ler cache do disco: {e}")
        return None, None, None, False

def atualizar_cache_manual(did, df):
    arquivo = _get_file_path(did)
    if os.path.exists(arquivo):
        try:
            with open(arquivo, 'r+', encoding='utf-8') as f:
                cached = json.load(f)
                cached['df_json'] = df.to_json(orient='records')
                cached['timestamp'] = time.time()
                
                f.seek(0)
                json.dump(cached, f)
                f.truncate()
        except Exception as e:
            print(f"Erro ao atualizar manual: {e}")