from flask import Flask, render_template, request, send_file, jsonify
import pandas as pd
import os
import json
import tempfile
import re
import threading
from datetime import datetime
from io import BytesIO

# Módulo de Conferência (Blueprint)
from conferencia_routes import conferencia_bp 

from xml_service import processar_pasta_xml
from db_repository import (
    buscar_dados_fornecedores, 
    buscar_filiais_sql,
    buscar_itens_pedidos_lote, 
    buscar_pedido_manual, 
    listar_fornecedores,
    listar_filiais_do_fornecedor,
    listar_pedidos_do_fornecedor,
    buscar_contato_fornecedor
)

app = Flask(__name__)
app.secret_key = 'vila_apps_secret_key'

# Registra o módulo de conferência
app.register_blueprint(conferencia_bp, url_prefix='/conferencia')

CAMINHO_FIXO = r"C:\Users\AS informática\Documents\Modelos Python\Confronto _ NFE\xml teste"

CACHE_ACERTO = os.path.join(tempfile.gettempdir(), 'vila_cache_acerto.json')
CACHE_DEVOLUCAO = os.path.join(tempfile.gettempdir(), 'vila_cache_devolucao.json')
CACHE_GERAL = os.path.join(tempfile.gettempdir(), 'vila_cache_geral.json')
ARQUIVO_STATUS = os.path.join(os.getcwd(), 'vila_status_db.json')

STATUS_GLOBAL = {'atual': 0, 'total': 0, 'status': 'parado', 'msg': ''}

def atualizar_progresso(atual, total):
    STATUS_GLOBAL['atual'] = atual
    STATUS_GLOBAL['total'] = total
    STATUS_GLOBAL['status'] = 'rodando'

def resetar_progresso():
    STATUS_GLOBAL['atual'] = 0
    STATUS_GLOBAL['total'] = 0
    STATUS_GLOBAL['status'] = 'iniciando'
    STATUS_GLOBAL['msg'] = ''

def carregar_status_db():
    if os.path.exists(ARQUIVO_STATUS):
        try:
            with open(ARQUIVO_STATUS, 'r', encoding='utf-8') as f:
                return json.load(f)
        except: return {}
    return {}

def salvar_status_db(dados):
    try:
        with open(ARQUIVO_STATUS, 'w', encoding='utf-8') as f:
            json.dump(dados, f, indent=4)
    except: pass

def limpar_cnpj(valor):
    if not valor: return ""
    return re.sub(r'\D', '', str(valor)).zfill(14) 

def formatar_moeda(val):
    try: return f"R$ {float(val):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except: return val

# --- LÓGICA CORRIGIDA: COMPARAÇÃO UNITÁRIA ---
def gerar_resumo_divergencia(nota):
    """
    Gera o resumo das divergências comparando PREÇO UNITÁRIO e QUANTIDADE.
    """
    itens_xml = nota.get('Itens', [])
    itens_erp = nota.get('Itens_ERP', [])
    
    # Indexa itens por ISBN para comparação rápida
    map_xml = {str(i.get('ISBN', '')).strip(): i for i in itens_xml}
    map_erp = {str(i.get('ISBN', '')).strip(): i for i in itens_erp}
    
    todos_isbns = set(map_xml.keys()) | set(map_erp.keys())
    
    c_preco = 0
    c_qtd = 0
    c_naoped = 0 # Item no XML que não está no Pedido

    for isbn in todos_isbns:
        x = map_xml.get(isbn)
        e = map_erp.get(isbn)
        
        if x and e:
            # --- QUANTIDADE ---
            qtd_x = float(x.get('Quantidade', 0))
            qtd_e = float(e.get('Quant', 0))
            
            if qtd_x != qtd_e:
                c_qtd += 1
            
            # --- PREÇO UNITÁRIO (CORREÇÃO SOLICITADA) ---
            # Calcula unitário líquido do XML (Total Liquido / Qtd)
            val_liq_total_x = float(x.get('Valor_Liquido', 0))
            val_unit_x = val_liq_total_x / qtd_x if qtd_x > 0 else 0
            
            # Unitário líquido do ERP (Vem direto da query como VlLiqUnit)
            val_unit_e = float(e.get('VlLiqUnit', 0))
            
            # Compara com tolerância de 1 centavo
            if abs(val_unit_x - val_unit_e) > 0.01:
                c_preco += 1
                
        elif x and not e:
            # Veio na nota, mas não tem no pedido (Sobra)
            c_naoped += 1

    # Monta texto resumo
    msgs = []
    if c_qtd > 0: msgs.append(f"Qtde Diferente ({c_qtd}x)")
    if c_preco > 0: msgs.append(f"Preço Diferente ({c_preco}x)")
    if c_naoped > 0: msgs.append(f"Item não consta no Pedido ({c_naoped}x)")
    
    if not msgs:
        if not itens_xml: return "XML Vazio"
        return "OK"
        
    return " | ".join(msgs)

def tarefa_processamento_background(modulo):
    if modulo == 'devolucao':
        cfops, tipo_pedido, arquivo_cache = ['5917', '6917'], 4, CACHE_DEVOLUCAO
    elif modulo == 'acerto':
        cfops, tipo_pedido, arquivo_cache = None, 1, CACHE_ACERTO
    else: 
        cfops, tipo_pedido, arquivo_cache = None, 1, CACHE_GERAL

    STATUS_GLOBAL['msg'] = 'Lendo arquivos XML...'
    df_xml, msg = processar_pasta_xml(CAMINHO_FIXO, cfops_filtro=cfops, callback_progresso=atualizar_progresso)
    
    if df_xml.empty:
        STATUS_GLOBAL['status'] = 'concluido'
        STATUS_GLOBAL['msg'] = msg or "Nenhum XML encontrado"
        return

    STATUS_GLOBAL['msg'] = 'Cruzando dados com ERP...'
    if 'CNPJ_Destinatario' in df_xml.columns:
        df_lojas = buscar_filiais_sql()
        if not df_lojas.empty:
            df_xml['KEY_CNPJ'] = df_xml['CNPJ_Destinatario'].apply(limpar_cnpj)
            df_lojas['KEY_CNPJ'] = df_lojas['CNPJ'].apply(limpar_cnpj)
            df_m = pd.merge(df_xml, df_lojas[['KEY_CNPJ', 'Nome_Filial']], on='KEY_CNPJ', how='left')
            df_m['Filial'] = df_m['Nome_Filial'].fillna('-')
            df_xml = df_m.drop(columns=['Nome_Filial'])
        else: df_xml['Filial'] = '-'
    else: df_xml['Filial'] = '-'

    df_forn = buscar_dados_fornecedores()
    if not df_forn.empty:
        df_xml['KEY_EMIT'] = df_xml['CNPJ_Emitente'].apply(limpar_cnpj)
        df_forn['KEY_EMIT'] = df_forn['CNPJ'].apply(limpar_cnpj)
        df_final = pd.merge(df_xml, df_forn, on='KEY_EMIT', how='left')
    else: df_final = df_xml.copy()

    cols = ['Nome_Fantasia', 'Filial', 'Prazo', 'Dia_Acerto']
    for c in cols:
        if c not in df_final.columns: df_final[c] = '-'
        else: df_final[c] = df_final[c].fillna('-')

    lista = df_final.to_dict('records')
    pedidos = [n.get('Numero_Pedido') for n in lista if n.get('Numero_Pedido')]
    
    df_itens_erp = pd.DataFrame()
    if pedidos:
        STATUS_GLOBAL['msg'] = f'Buscando {len(pedidos)} pedidos no Banco...'
        df_itens_erp = buscar_itens_pedidos_lote(pedidos, tipo_acerto_alvo=tipo_pedido)
        if not df_itens_erp.empty:
            for col in df_itens_erp.select_dtypes(include=['datetime', 'datetimetz']).columns:
                df_itens_erp[col] = df_itens_erp[col].astype(str)

    STATUS_GLOBAL['msg'] = 'Finalizando análises...'
    for nota in lista:
        ped = str(nota.get('Numero_Pedido', ''))
        nota['Itens_ERP'] = []
        if ped and not df_itens_erp.empty:
            itens = df_itens_erp[df_itens_erp['Numero_Pedido_Chave'] == ped]
            if not itens.empty: nota['Itens_ERP'] = itens.to_dict('records')
        
        # Calcula divergência com a nova lógica de Valor Unitário
        nota['Divergencia_Resumo'] = gerar_resumo_divergencia(nota)

        if 'Valor_Total' in nota: nota['Valor_Total'] = formatar_moeda(nota['Valor_Total'])

    ts = datetime.now().strftime("%d/%m/%Y às %H:%M")
    try:
        with open(arquivo_cache, 'w', encoding='utf-8') as f:
            json.dump({"timestamp": ts, "dados": lista}, f, ensure_ascii=False, indent=4)
    except: pass

    STATUS_GLOBAL['status'] = 'concluido'
    STATUS_GLOBAL['msg'] = 'Concluído!'

def ler_cache(caminho):
    if os.path.exists(caminho):
        try:
            with open(caminho, 'r', encoding='utf-8') as f:
                c = json.load(f)
                return c.get('dados', []), c.get('timestamp', '-')
        except: pass
    return [], None

# --- ROTAS ---
@app.route('/api/iniciar_processamento', methods=['POST'])
def api_iniciar_processamento():
    modulo = request.json.get('modulo', 'geral')
    if STATUS_GLOBAL['status'] == 'rodando':
        return jsonify({'status': 'ocupado', 'msg': 'Já existe um processo rodando.'})
    resetar_progresso()
    thread = threading.Thread(target=tarefa_processamento_background, args=(modulo,))
    thread.start()
    return jsonify({'status': 'iniciado'})

@app.route('/api/progresso')
def api_progresso():
    pct = 0
    if STATUS_GLOBAL['total'] > 0:
        pct = int((STATUS_GLOBAL['atual'] / STATUS_GLOBAL['total']) * 100)
    return jsonify({
        'atual': STATUS_GLOBAL['atual'],
        'total': STATUS_GLOBAL['total'],
        'percentual': pct,
        'status': STATUS_GLOBAL['status'],
        'msg': STATUS_GLOBAL['msg']
    })

@app.route('/')
def menu(): return render_template('menu.html')

@app.route('/leitor_xml')
def leitor_xml():
    dados, ts = ler_cache(CACHE_GERAL)
    lojas = []
    try:
        df = buscar_filiais_sql()
        if not df.empty: lojas = sorted(df['Nome_Filial'].unique().tolist())
    except: pass
    return render_template('leitor_geral.html', dados=dados, ultima_atualizacao=ts, lista_lojas=lojas)

@app.route('/workflow')
def workflow():
    dados, ts = ler_cache(CACHE_GERAL)
    db_st = carregar_status_db()
    lista_filtrada = []
    if dados:
        for r in dados:
            chave = r.get('Chave_Acesso')
            status_atual = db_st.get(chave, 'PENDENTE')
            r['Status_Workflow'] = status_atual
            if 'Divergencia_Resumo' not in r: r['Divergencia_Resumo'] = gerar_resumo_divergencia(r)
            if status_atual in ['EM ANÁLISE', 'CONCLUÍDO']: lista_filtrada.append(r)
    lojas = []
    try:
        df = buscar_filiais_sql()
        if not df.empty: lojas = sorted(df['Nome_Filial'].unique().tolist())
    except: pass
    return render_template('workflow.html', dados=lista_filtrada, ultima_atualizacao=ts, lista_lojas=lojas)

@app.route('/acerto')
def acerto():
    dados, ts = ler_cache(CACHE_ACERTO)
    if dados:
        db_st = carregar_status_db()
        for r in dados:
            r['Status_Workflow'] = db_st.get(r.get('Chave_Acesso'), 'PENDENTE')
    return render_template('acerto.html', dados=dados, ultima_atualizacao=ts)

@app.route('/devolucao')
def devolucao():
    dados, ts = ler_cache(CACHE_DEVOLUCAO)
    return render_template('devolucao.html', dados=dados, ultima_atualizacao=ts)

# --- APIs de rotas---

@app.route('/api/dados_fornecedor')
def api_dados_fornecedor():
    cod = request.args.get('cod_cli')
    if not cod: return jsonify([])
    return jsonify(buscar_contato_fornecedor(cod))

@app.route('/api/atualizar_status', methods=['POST'])
def api_atualizar_status():
    try:
        p = request.json
        db = carregar_status_db()
        db[p['chave']] = p['status']
        salvar_status_db(db)
        return jsonify({'success': True})
    except: return jsonify({'success': False})

@app.route('/api/fornecedores')
def api_fornecedores(): return jsonify(listar_fornecedores(request.args.get('tipo', 1)))
@app.route('/api/filiais')
def api_filiais(): return jsonify(listar_filiais_do_fornecedor(request.args.get('cod_cli'), request.args.get('tipo', 1)))
@app.route('/api/pedidos')
def api_pedidos():
    a = request.args
    df = pd.DataFrame(listar_pedidos_do_fornecedor(a.get('cod_cli'), a.get('cod_filial'), a.get('data_ini'), a.get('data_fim'), a.get('tipo', 1)))
    if not df.empty:
        for c in df.select_dtypes(include=['datetime']).columns: df[c] = df[c].astype(str)
        return jsonify(df.to_dict('records'))
    return jsonify([])
@app.route('/api/buscar_pedido')
def api_buscar_pedido():
    df = buscar_pedido_manual(request.args.get('pedido'), request.args.get('tipo', 1))
    if not df.empty:
        for c in df.select_dtypes(include=['datetime']).columns: df[c] = df[c].astype(str)
        return jsonify(df.to_dict('records'))
    return jsonify([])
@app.route('/exportar_excel', methods=['POST'])
def exportar_excel():
    origem = request.form.get('origem', 'geral')
    mapa = {'acerto': CACHE_ACERTO, 'devolucao': CACHE_DEVOLUCAO, 'geral': CACHE_GERAL}
    arquivo = mapa.get(origem, CACHE_GERAL)
    if not os.path.exists(arquivo): return "Sem dados.", 404
    try:
        with open(arquivo, 'r', encoding='utf-8') as f: dados = json.load(f).get('dados', [])
        db_st = carregar_status_db()
        for d in dados: 
            d['Status'] = db_st.get(d.get('Chave_Acesso'), 'PENDENTE')
            if 'Divergencia_Resumo' not in d: d['Divergencia_Resumo'] = gerar_resumo_divergencia(d)
        df = pd.DataFrame(dados)
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')
        cols = ['Data_Emissao', 'Status', 'Numero_NF', 'Serie', 'Numero_Pedido', 'CFOPs', 'Nome_Fantasia', 'Filial', 'Prazo', 'Dia_Acerto', 'CNPJ_Emitente', 'Valor_Total', 'Chave_Acesso', 'Divergencia_Resumo']
        valid = [c for c in cols if c in df.columns]
        df[valid].to_excel(writer, sheet_name='Notas', index=False)
        writer.close()
        output.seek(0)
        return send_file(output, download_name=f"Relatorio_{origem}.xlsx", as_attachment=True)
    except Exception as e: return f"Erro: {e}", 500

if __name__ == '__main__':
    app.run(debug=True, port=5002)