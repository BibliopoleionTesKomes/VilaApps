from flask import Blueprint, render_template, request, redirect, url_for, session, send_file, jsonify, flash
import pandas as pd
import uuid
import time
from io import BytesIO, StringIO

# Importa a lógica do arquivo conferencia.py (que deve estar na mesma pasta)
from conferencia import (
    carregar_acerto, carregar_venda, carregar_venda_acao, buscar_vendas_sql,
    gerar_planilha_conferencia, gerar_resumo_consolidado, buscar_acerto_sql,
    processar_acerto_sql, buscar_fornecedores_sql, buscar_pedidos_sql, calcular_qtd_final,
    carregar_quebra_inventario
)

conferencia_bp = Blueprint('conferencia', __name__)

DATA_CACHE = {}
SESSION_EXPIRY_TIME = 3600

def formatar_valor(val):
    return f"R$ {val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def formatar_moeda_df(df):
    df_fmt = df.copy()
    cols = ['Vl. Unit._acerto', 'Vl. Unit._venda', 'Preco_Venda_F', 'Vlr. Liq. Qtd. Divergência', 'Vlr. Liq. A Acertar', 'Vlr. Quebra Liquida', 'Vlr. Quebra Bruta']
    for col in cols:
        if col in df_fmt.columns:
            df_fmt[col] = pd.to_numeric(df_fmt[col], errors='coerce').fillna(0).apply(
                lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            )
    return df_fmt

def get_cached_data(data_id):
    if data_id not in DATA_CACHE: return None, None, None, False
    cached = DATA_CACHE[data_id]
    cached['timestamp'] = time.time()
    df = pd.read_json(StringIO(cached['df_json']), orient='records')
    venda_sum = pd.read_json(StringIO(cached['venda_summary_json']), orient='records')
    return df, cached['fornecedor'], venda_sum, cached.get('has_quebra', False)

@conferencia_bp.route('/', methods=['GET', 'POST'])
def index():
    now = time.time()
    keys = [k for k, v in DATA_CACHE.items() if now - v['timestamp'] > SESSION_EXPIRY_TIME]
    for k in keys: del DATA_CACHE[k]

    if request.method == 'POST':
        try:
            source = request.form.get('source', 'excel')
            df_acerto = pd.DataFrame()
            fornecedor = "Indefinido"
            df_venda = pd.DataFrame(columns=['filial', 'ISBN', 'Quant_venda', 'Vl. Unit._venda', 'Preco_Venda_F'])
            df_acao = pd.DataFrame(columns=['filial', 'ISBN', 'Quant_acao'])
            df_quebra = pd.DataFrame(columns=['filial', 'ISBN', 'Quebra_Inv'])
            venda_sum = pd.DataFrame(columns=['filial', 'Venda Bruta'])
            has_quebra = False

            if source == 'sql':
                pedidos = request.form.getlist('pedidos_selecionados')
                if not pedidos:
                    flash('Selecione ao menos um pedido.', 'error')
                    return redirect(url_for('conferencia.index'))
                
                df_raw = buscar_acerto_sql(pedidos)
                if df_raw.empty:
                    flash('Dados de acerto não encontrados.', 'error')
                    return redirect(url_for('conferencia.index'))
                
                df_acerto, _, fornecedor = processar_acerto_sql(df_raw)
                
                d_ini, d_fim = request.form.get('data_inicio_vendas'), request.form.get('data_fim_vendas')
                forn_id = request.form.get('fornecedor_id')
                df_venda = buscar_vendas_sql(d_ini, d_fim, forn_id)
                
                d_ini_ac, d_fim_ac = request.form.get('data_inicio_vendas_acao'), request.form.get('data_fim_vendas_acao')
                if d_ini_ac and d_fim_ac:
                    df_acao_raw = buscar_vendas_sql(d_ini_ac, d_fim_ac, forn_id)
                    if not df_acao_raw.empty:
                        df_acao = df_acao_raw.rename(columns={'Quant_venda': 'Quant_acao'})[['filial', 'ISBN', 'Quant_acao']]
                
                if not df_venda.empty:
                    venda_sum = df_venda.groupby('filial', as_index=False).agg({'Vl. Unit._venda': 'sum'}).rename(columns={'Vl. Unit._venda': 'Venda Bruta'})

                f_quebra = request.files.get('quebra_file')
                if f_quebra and f_quebra.filename != '':
                    df_quebra = carregar_quebra_inventario(BytesIO(f_quebra.read()))
                    if not df_quebra.empty: has_quebra = True

            else:
                files = request.files.getlist('acerto_files')
                if not files or files[0].filename == '':
                    flash('Arquivo de Acerto obrigatório.', 'error')
                    return redirect(url_for('conferencia.index'))
                
                dfs = []
                for f in files:
                    s = BytesIO(f.read())
                    d, _, forn = carregar_acerto(s)
                    if not d.empty:
                        dfs.append(d)
                        if fornecedor == "Indefinido": fornecedor = forn
                
                if not dfs:
                    flash('Arquivos inválidos.', 'error')
                    return redirect(url_for('conferencia.index'))
                
                df_acerto = pd.concat(dfs, ignore_index=True)

                f_venda = request.files.get('venda_file')
                f_acao = request.files.get('venda_acao_file')
                f_quebra = request.files.get('quebra_file')

                if f_venda:
                    df_venda = carregar_venda(BytesIO(f_venda.read()))
                    if not df_venda.empty:
                        venda_sum = df_venda.groupby('filial', as_index=False).agg({'Vl. Unit._venda': 'sum'}).rename(columns={'Vl. Unit._venda': 'Venda Bruta'})
                if f_acao:
                    df_acao = carregar_venda_acao(BytesIO(f_acao.read()))
                if f_quebra and f_quebra.filename != '':
                    df_quebra = carregar_quebra_inventario(BytesIO(f_quebra.read()))
                    if not df_quebra.empty: has_quebra = True

            promos = request.form.get('isbns_promocionais', '')
            df_res = gerar_planilha_conferencia(df_acerto, df_venda, df_acao, promos, df_quebra)
            
            if df_res.empty:
                flash('Sem correspondência de dados.', 'error')
                return redirect(url_for('conferencia.index'))
            
            did = str(uuid.uuid4())
            DATA_CACHE[did] = {
                'df_json': df_res.to_json(orient='records'),
                'fornecedor': fornecedor,
                'timestamp': time.time(),
                'venda_summary_json': venda_sum.to_json(orient='records'),
                'has_quebra': has_quebra
            }
            session['data_id'] = did
            return redirect(url_for('conferencia.show_results'))

        except Exception as e:
            print(e)
            flash(f'Erro: {e}', 'error')
            return redirect(url_for('conferencia.index'))

    return render_template('conf_index.html', fornecedores=buscar_fornecedores_sql())

@conferencia_bp.route('/results')
def show_results():
    if 'data_id' not in session: return redirect(url_for('conferencia.index'))
    df, forn, sum_df, has_quebra = get_cached_data(session['data_id'])
    if df is None: return redirect(url_for('conferencia.index'))
    
    df = calcular_qtd_final(df)
    
    filial = request.args.get('filial', 'all').strip().lower() or 'all'
    filt = request.args.get('filter', 'all')
    
    df_show = df.copy()
    if filial != 'all': df_show = df_show[df_show['filial'] == filial]
    
    if filt == 'qty': df_show = df_show[df_show['Situação Qtd.'] == 'Divergência']
    elif filt == 'price': df_show = df_show[df_show['Situação Preço'] == 'Divergência']
    elif filt == 'acao': df_show = df_show[df_show['Situação Qtd.'] == 'Ação']
    
    df_show = df_show.sort_values(by=['filial', 'Titulo'])

    v_bruta = 0
    if not sum_df.empty:
        sum_df['filial'] = sum_df['filial'].astype(str).str.strip().str.lower()
        v_bruta = sum_df['Venda Bruta'].sum() if filial == 'all' else sum_df[sum_df['filial'] == filial]['Venda Bruta'].sum()

    return render_template('conf_results.html', 
                           data=formatar_moeda_df(df_show).to_dict('records'), 
                           fornecedor=forn, 
                           filiais_list=sorted(df['filial'].unique()), 
                           current_filial=filial, 
                           current_filter=filt,
                           venda_bruta_total=formatar_valor(v_bruta),
                           has_quebra=has_quebra)

@conferencia_bp.route('/api/get_pedidos', methods=['POST'])
def api_get_pedidos():
    d = request.get_json()
    return jsonify(buscar_pedidos_sql(d.get('fornecedor_id'), d.get('status_id'), d.get('data_ini'), d.get('data_fim')))

@conferencia_bp.route('/update_manual', methods=['POST'])
def update_manual_acerto():
    if 'data_id' not in session: return jsonify({'message': 'Sessão expirada'}), 400
    did = session['data_id']
    df, forn, sum_df, _ = get_cached_data(did)
    
    updates = request.get_json()
    if not updates:
        df = calcular_qtd_final(df)
        res, tots = gerar_resumo_consolidado(df, sum_df)
        return jsonify({'success': True, 'totais': {k: formatar_valor(v) for k, v in tots.items()}})

    up_map = {(u['filial'].strip().lower(), str(u['isbn']).strip()): int(u['qtd']) for u in updates}
    
    def update_row(row):
        key = (str(row['filial']).strip().lower(), str(row['ISBN']).strip())
        return up_map.get(key, row['Qtd. a Acertar'])
        
    df['Qtd. a Acertar'] = df.apply(update_row, axis=1)
    DATA_CACHE[did]['df_json'] = df.to_json(orient='records')
    DATA_CACHE[did]['timestamp'] = time.time()
    
    df = calcular_qtd_final(df)
    res, tots = gerar_resumo_consolidado(df, sum_df)
    
    return jsonify({'success': True, 'totais': {k: formatar_valor(v) for k, v in tots.items()}})

@conferencia_bp.route('/summary')
def show_summary():
    if 'data_id' not in session: return redirect(url_for('conferencia.index'))
    df, forn, sum_df, _ = get_cached_data(session['data_id'])
    if df is None: return redirect(url_for('conferencia.index'))
    
    df = calcular_qtd_final(df)
    res, tots = gerar_resumo_consolidado(df, sum_df)
    
    res_fmt = res.copy()
    for c in res_fmt.columns:
        if c != 'filial': res_fmt[c] = res_fmt[c].apply(formatar_valor)
            
    return render_template('conf_summary.html', resumo_por_filial=res_fmt.to_dict('records'), totais_gerais={k: formatar_valor(v) for k, v in tots.items()}, fornecedor=forn)

@conferencia_bp.route('/download')
def download_file():
    if 'data_id' not in session: return redirect(url_for('conferencia.index'))
    df, forn, sum_df, _ = get_cached_data(session['data_id'])
    if df is None: return redirect(url_for('conferencia.index'))
    
    df = calcular_qtd_final(df)
    out = BytesIO()
    with pd.ExcelWriter(out, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Conferencia', index=False)
        res, _ = gerar_resumo_consolidado(df, sum_df)
        res.to_excel(writer, sheet_name='Resumo', index=False)
    out.seek(0)
    return send_file(out, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', as_attachment=True, download_name=f'Conferencia_{forn}.xlsx')

@conferencia_bp.route('/download_export')
def download_export_list():
    if 'data_id' not in session: return redirect(url_for('conferencia.index'))
    df, forn, _, _ = get_cached_data(session['data_id'])
    df = calcular_qtd_final(df)
    
    filial = request.args.get('filial', 'all')
    filter_type = request.args.get('filter', 'all')
    
    if filial != 'all': df = df[df['filial'] == filial]
    if filter_type == 'acao': df = df[df['Item Promocional'] == 'Sim']

    df = df[df['Qtd. Final'] > 0].sort_values(['filial', 'ISBN'])
    
    out = BytesIO()
    with pd.ExcelWriter(out, engine='xlsxwriter') as writer:
        df[['filial', 'ISBN', 'Qtd. Final']].to_excel(writer, sheet_name='Export', index=False)
    out.seek(0)
    return send_file(out, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', as_attachment=True, download_name=f'Export_{forn}.xlsx')

@conferencia_bp.route('/show_export_list')
def show_export_list():
    if 'data_id' not in session: return redirect(url_for('conferencia.index'))
    df, forn, _, _ = get_cached_data(session['data_id'])
    if df is None: return redirect(url_for('conferencia.index'))
    
    df = calcular_qtd_final(df)
    
    filial = request.args.get('filial', 'all')
    filter_type = request.args.get('filter', 'all')

    if filial != 'all': df = df[df['filial'] == filial]
    if filter_type == 'acao': df = df[df['Item Promocional'] == 'Sim']

    df = df[df['Qtd. Final'] > 0].sort_values(['filial', 'ISBN'])
    
    return render_template('conf_export_list.html', 
                           data=df.to_dict('records'), 
                           filiais_list=sorted(df['filial'].unique()), 
                           current_filial=filial,
                           current_filter=filter_type, 
                           fornecedor=forn)