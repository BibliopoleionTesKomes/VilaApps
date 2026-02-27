# --- IMPORTAÇÕES ---

# Pandas: Biblioteca poderosa para manipulação de dados (tabelas, excel, csv).
import pandas as pd
# Time: Para gerar timestamps (marcas de tempo) úteis para cache.
import time
# Flask: Framework web. Importamos ferramentas para rotas, templates, redirecionamento, sessão, mensagens e arquivos.
from flask import Blueprint, render_template, request, redirect, url_for, session, flash, jsonify, send_file
# BytesIO: Permite tratar arquivos na memória RAM (sem salvar no disco rígido), o que é mais rápido e seguro.
from io import BytesIO

# Importações dos nossos Serviços e Repositórios (Camada de Lógica e Dados)
# Service: Onde fica a regra de negócio (cálculos, processamento).
# Repository: Onde fica o acesso ao banco de dados (SQL).
from app.services.cache_service import ler_cache
from app.repository.geral_repo import buscar_filiais, listar_fornecedores
from app.repository.conferencia_repo import buscar_pedidos_para_conferencia
from app.services.conferencia_service import (
    processar_acerto_sql_service, processar_vendas_sql_service,
    carregar_acerto_excel, carregar_venda_excel, carregar_quebra_inventario,
    calcular_conferencia_padrao, cache_save, cache_get,
    gerar_planilha_acao, calcular_qtd_final_acao, gerar_resumo_acao,
    calcular_qtd_final, gerar_resumo_consolidado,
    atualizar_cache_manual  # <--- (NOVO) Função necessária para salvar edições no disco
)

# Criação do Blueprint 'conferencia'
# Isso isola todas as rotas deste módulo. Prefixo URL será definido no create_app (ex: /conferencia)
conferencia_bp = Blueprint('conferencia', __name__)

# --- ROTAS DE PÁGINAS INICIAIS (INPUT DE DADOS) ---

@conferencia_bp.route('/')
def index():
    """
    Página Inicial da Conferência Padrão (Acerto vs Venda).
    Carrega a lista de fornecedores para preencher o combobox da tela.
    """
    fornecedores = listar_fornecedores()
    return render_template('conf_index.html', fornecedores=fornecedores)

@conferencia_bp.route('/acao')
def index_acao():
    """
    Página Inicial da Conferência de Ação Promocional.
    É uma tela separada porque a lógica de negócio é ligeiramente diferente.
    """
    fornecedores = listar_fornecedores()
    return render_template('conf_acao_index.html', fornecedores=fornecedores)

@conferencia_bp.route('/leitor_xml')
def leitor_xml():
    """
    Tela do Leitor Geral de XMLs.
    Exibe dados processados previamente (cache 'geral') e lista de lojas disponíveis.
    """
    dados, ts = ler_cache('geral')
    lojas = []
    try:
        # Busca filiais no banco para o filtro lateral
        df = buscar_filiais()
        if not df.empty: lojas = sorted(df['Nome_Filial'].unique().tolist())
    except: 
        pass # Se falhar o banco, a tela carrega sem a lista de lojas, sem travar.
    return render_template('leitor_geral.html', dados=dados, ultima_atualizacao=ts, lista_lojas=lojas)

# --- ROTAS DE API AUXILIAR (AJAX) ---

@conferencia_bp.route('/api/get_pedidos', methods=['POST'])
def api_get_pedidos():
    """
    Rota chamada via JavaScript (fetch) para buscar pedidos no SQL sem recarregar a página.
    Recebe JSON com filtros e devolve JSON com a lista de pedidos.
    """
    d = request.get_json()
    return jsonify(buscar_pedidos_para_conferencia(
        d.get('fornecedor_id'), 
        d.get('status_id'), 
        d.get('data_ini'), 
        d.get('data_fim')
    ))

# --- ROTA CENTRAL DE PROCESSAMENTO ---

@conferencia_bp.route('/iniciar_processamento', methods=['POST'])
def iniciar_processamento():
    """
    O 'Cérebro' do sistema. Recebe o formulário (SQL ou Excel), processa os dados
    e salva o resultado na memória (Cache) para ser exibido na tela de resultados.
    """
    try:
        # Detecta se estamos na tela de 'Ação' verificando a URL de origem (referrer)
        is_acao = '/acao' in request.referrer or 'acao' in request.referrer
        
        # Identifica a origem dos dados: 'sql' (Banco) ou 'excel' (Upload)
        source = request.form.get('source', 'excel')
        
        # DataFrames iniciais vazios
        df_acerto = pd.DataFrame()
        fornecedor = "Indefinido"
        df_venda = pd.DataFrame()
        df_acao = pd.DataFrame()
        df_quebra = pd.DataFrame()
        venda_sum = pd.DataFrame()
        has_quebra = False

        # --- FLUXO 1: DADOS VIA SQL (BANCO DE DADOS) ---
        if source == 'sql':
            # 1. Pega pedidos selecionados pelo usuário
            pedidos = request.form.getlist('pedidos_selecionados')
            if not pedidos:
                flash('Selecione ao menos um pedido.', 'error')
                return redirect(request.referrer)
            
            # 2. Busca os itens de acerto (Devolução/Entrada) no Banco
            df_acerto, _, fornecedor = processar_acerto_sql_service(pedidos)
            
            # 3. Busca vendas no período
            d_ini, d_fim = request.form.get('data_inicio_vendas'), request.form.get('data_fim_vendas')
            forn_id = request.form.get('fornecedor_id')
            
            if not is_acao:
                # Se for conferência padrão, busca venda geral
                df_venda = processar_vendas_sql_service(d_ini, d_fim, forn_id)
            
            # 4. Se houver período de Ação definido, busca venda específica da ação
            d_ini_ac, d_fim_ac = request.form.get('data_inicio_vendas_acao'), request.form.get('data_fim_vendas_acao')
            if d_ini_ac and d_fim_ac:
                df_acao_raw = processar_vendas_sql_service(d_ini_ac, d_fim_ac, forn_id)
                if not df_acao_raw.empty:
                    # Renomeia para evitar conflito de colunas e seleciona apenas o necessário
                    df_acao = df_acao_raw.rename(columns={'Quant_venda': 'Quant_acao'})[['filial', 'ISBN', 'Quant_acao']]
                    if is_acao:
                        # Calcula totais brutos para o cabeçalho do resumo
                        venda_sum = df_acao_raw.groupby('filial', as_index=False).agg({'Vl. Unit._venda': 'sum'}).rename(columns={'Vl. Unit._venda': 'Venda Bruta'})

            # 5. Processa arquivos de quebra (MÚLTIPLOS)
            files_quebra = request.files.getlist('quebra_file')
            dfs_quebra_temp = []
            
            for f_q in files_quebra:
                if f_q.filename == '': continue
                # Processa cada arquivo individualmente para ler o cabeçalho daquela filial
                df_temp = carregar_quebra_inventario(BytesIO(f_q.read()))
                if not df_temp.empty:
                    dfs_quebra_temp.append(df_temp)
            
            # Junta todos os DataFrames de quebra em um só
            if dfs_quebra_temp:
                df_quebra = pd.concat(dfs_quebra_temp, ignore_index=True)
                has_quebra = True

        # --- FLUXO 2: DADOS VIA EXCEL (UPLOAD) ---
        else: 
            # 1. Carrega arquivo(s) de Acerto (Obrigatório)
            files = request.files.getlist('acerto_files')
            if not files or files[0].filename == '':
                flash('Arquivo de Acerto obrigatório.', 'error')
                return redirect(request.referrer)
            
            # Permite múltiplos arquivos de acerto (ex: várias notas fiscais)
            dfs = []
            for f in files:
                s = BytesIO(f.read())
                d, _, forn = carregar_acerto_excel(s)
                if not d.empty:
                    dfs.append(d)
                    if fornecedor == "Indefinido": fornecedor = forn
            
            if dfs: df_acerto = pd.concat(dfs, ignore_index=True)

            # 2. Carrega arquivo de Venda Geral
            f_venda = request.files.get('venda_file')
            if f_venda:
                df_venda = carregar_venda_excel(BytesIO(f_venda.read()), 'Quant_venda')
            
            # 3. Carrega arquivo de Venda Ação
            f_acao = request.files.get('venda_acao_file')
            if f_acao:
                df_acao = carregar_venda_excel(BytesIO(f_acao.read()), 'Quant_acao')
                if not df_acao.empty: df_acao = df_acao[['filial', 'ISBN', 'Quant_acao']]

            # 4. Carrega arquivo de Quebra (MÚLTIPLOS)
            files_quebra = request.files.getlist('quebra_file')
            dfs_quebra_temp = []
            
            for f_q in files_quebra:
                if f_q.filename == '': continue
                df_temp = carregar_quebra_inventario(BytesIO(f_q.read()))
                if not df_temp.empty:
                    dfs_quebra_temp.append(df_temp)
            
            if dfs_quebra_temp:
                df_quebra = pd.concat(dfs_quebra_temp, ignore_index=True)
                has_quebra = True

        # Calcula totais de venda se houver dados
        if not is_acao and not df_venda.empty:
            venda_sum = df_venda.groupby('filial', as_index=False).agg({'Vl. Unit._venda': 'sum'}).rename(columns={'Vl. Unit._venda': 'Venda Bruta'})

        # Pega a lista de ISBNs promocionais (colada no textarea)
        promos = request.form.get('isbns_promocionais', '')
        
        # --- CÁLCULO FINAL (CRUZAMENTO DE DADOS) ---
        if is_acao:
            # Lógica específica para Ação Promocional
            df_res = gerar_planilha_acao(df_acerto, df_acao, promos, df_quebra)
            if df_res.empty:
                flash('Sem itens promocionais. Verifique os ISBNs.', 'error')
                return redirect(url_for('conferencia.index_acao'))
        else:
            # Lógica Padrão (Acerto - Venda - Quebra)
            df_res = calcular_conferencia_padrao(df_acerto, df_venda, df_acao, promos, df_quebra)
        
        if df_res.empty:
            flash('Sem correspondência de dados.', 'error')
            return redirect(request.referrer)
        
        # --- SALVAMENTO EM CACHE (AGORA EM DISCO) ---
        # Salvamos o DataFrame processado no DISCO (não mais RAM) e guardamos o ID na sessão.
        did = cache_save(df_res, fornecedor, venda_sum, has_quebra)
        session['data_id'] = did
        
        # Redireciona para a tela de resultados correta
        if is_acao:
            return redirect(url_for('conferencia.show_results_acao'))
        else:
            return redirect(url_for('conferencia.show_results'))

    except Exception as e:
        print(f"Erro processamento: {e}")
        flash(f'Erro no processamento: {e}', 'error')
        return redirect(request.referrer)

# --- ROTAS DE EXIBIÇÃO DE RESULTADOS ---

def formatar_valor(val):
    """Auxiliar para formatar moeda R$ no padrão brasileiro"""
    return f"R$ {val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

@conferencia_bp.route('/results')
def show_results():
    """
    Exibe a tabela de resultados da Conferência Padrão.
    Recupera os dados do Cache usando o ID salvo na sessão.
    """
    if 'data_id' not in session: return redirect(url_for('conferencia.index'))
    
    # Recupera o DataFrame original do cache (DO DISCO)
    df, forn, sum_df, has_quebra = cache_get(session['data_id'])
    has_quebra=True #temporario pra descobrir quebra, depois tirar
    if df is None: return redirect(url_for('conferencia.index'))
    
    # Recalcula a coluna 'Qtd. a Acertar' (caso tenha lógica dinâmica)
    df = calcular_qtd_final(df)
    
    # Filtros visuais (Filial e Tipo de Divergência)
    filial_arg = request.args.get('filial', 'all').strip().lower() or 'all'
    filt = request.args.get('filter', 'all')
    
    df_show = df.copy()
    
    # Normaliza nomes de filial para garantir compatibilidade
    if 'filial' in df_show.columns:
        df_show['filial'] = df_show['filial'].astype(str).str.strip().str.lower()
        
    if filial_arg != 'all': 
        df_show = df_show[df_show['filial'] == filial_arg]
    
    # Aplica filtros de visualização
    if filt == 'qty': df_show = df_show[df_show['Situação Qtd.'] == 'Divergência']
    elif filt == 'price' and 'Situação Preço' in df_show.columns: 
        df_show = df_show[df_show['Situação Preço'] == 'Divergência']
    elif filt == 'acao': df_show = df_show[df_show['Situação Qtd.'] == 'Ação']
    
    df_show = df_show.sort_values(by=['filial', 'Titulo'])

    # --- CORREÇÃO DO VALOR DE VENDA ---
    # Prepara o objeto de vendas totais por filial vindo do sum_df (que tem a venda integral)
    vendas_por_filial = {}
    v_bruta_total_kpi = 0
    
    if not sum_df.empty:
        # Garante normalização para bater com as chaves do front
        sum_df['filial'] = sum_df['filial'].astype(str).str.strip().str.lower()
        
        # Cria dicionário { 'nome_filial': valor_float }
        vendas_por_filial = sum_df.set_index('filial')['Venda Bruta'].to_dict()
        
        # Calcula KPI do card (topo da tela)
        if filial_arg == 'all':
            v_bruta_total_kpi = sum_df['Venda Bruta'].sum()
        else:
            v_bruta_total_kpi = vendas_por_filial.get(filial_arg, 0)

    return render_template('conf_results.html', 
                           data=df_show.to_dict('records'), 
                           fornecedor=forn, 
                           filiais_list=sorted(df['filial'].unique()), 
                           current_filial=filial_arg, 
                           current_filter=filt,
                           venda_bruta_total=formatar_valor(v_bruta_total_kpi),
                           vendas_por_filial=vendas_por_filial,
                           exibir_quebra=has_quebra) # <-- MUDADO: De 'has_quebra' para 'exibir_quebra'

@conferencia_bp.route('/acao/results')
def show_results_acao():
    """
    Exibe a tabela de resultados da Conferência de Ação Promocional.
    Similar à rota acima, mas usa templates e cálculos específicos de ação.
    """
    if 'data_id' not in session: return redirect(url_for('conferencia.index_acao'))
    
    # Recupera do Disco
    df, forn, sum_df, has_quebra = cache_get(session['data_id'])
    if df is None: return redirect(url_for('conferencia.index_acao'))
    
    df = calcular_qtd_final_acao(df)
    
    filial = request.args.get('filial', 'all').strip().lower() or 'all'
    
    df_show = df.copy()
    if filial != 'all': df_show = df_show[df_show['filial'] == filial]
    df_show = df_show.sort_values(by=['filial', 'Titulo'])

    v_bruta = 0
    if not sum_df.empty:
        sum_df['filial'] = sum_df['filial'].astype(str).str.strip().str.lower()
        v_bruta = sum_df['Venda Bruta'].sum() if filial == 'all' else sum_df[sum_df['filial'] == filial]['Venda Bruta'].sum()

    return render_template('conf_acao_results.html', 
                           data=df_show.to_dict('records'), 
                           fornecedor=forn, 
                           filiais_list=sorted(df['filial'].unique()), 
                           current_filial=filial, 
                           venda_bruta_total=formatar_valor(v_bruta),
                           has_quebra=has_quebra)

# --- ROTA DE ATUALIZAÇÃO MANUAL (CORRIGIDA) ---

@conferencia_bp.route('/update_manual', methods=['POST'])
def update_manual_acerto():
    """
    Rota AJAX chamada quando o usuário altera manualmente a 'Qtd. a Acertar' na tabela.
    Atualiza o DataFrame no Cache do servidor para que a exportação Excel pegue os novos valores.
    """
    if 'data_id' not in session: return jsonify({'message': 'Sessão expirada'}), 400
    did = session['data_id']
    
    # Pega o DF do arquivo no disco
    # O cache_get retorna uma tupla (df, fornecedor, venda_sum, has_quebra), ignoramos o resto com _
    df, _, _, _ = cache_get(did) 
    
    if df is None: return jsonify({'message': 'Dados não encontrados'}), 404

    updates = request.get_json() # Lista de alterações [{filial, isbn, qtd}, ...]
    if updates:
        
        # --- FUNÇÃO DE LIMPEZA ABSOLUTA ---
        # Resolve o problema de 9781234.0 (float) vs "9781234" (string)
        # Garante que SEMPRE vamos comparar texto limpo com texto limpo
        def clean_key_val(val):
            # Converte pra string, divide no ponto (remove decimal) e tira espaços
            return str(val).split('.')[0].strip().lower()

        # Função para converter a quantidade em Inteiro seguro
        def safe_int(val):
            try:
                if not val: return 0
                return int(float(val))
            except: return 0

        # 1. Cria o Dicionário de Atualização usando a CHAVE LIMPA
        # Ex: { ('loja1', '978853590'): 10 }
        up_map = {}
        for u in updates:
            chave = (clean_key_val(u['filial']), clean_key_val(u['isbn']))
            up_map[chave] = safe_int(u['qtd'])
        
        # 2. Aplica a atualização linha a linha no DataFrame
        def update_row(row):
            # Gera a chave da linha atual usando a MESMA limpeza
            chave_linha = (clean_key_val(row['filial']), clean_key_val(row['ISBN']))
            
            # Se a chave bater, retorna o novo valor. Se não, mantém o antigo.
            return up_map.get(chave_linha, row['Qtd. a Acertar'])
        
        # Aplica a atualização linha a linha
        df['Qtd. a Acertar'] = df.apply(update_row, axis=1)
        
        # --- CORREÇÃO: Salva de volta no arquivo físico ---
        atualizar_cache_manual(did, df)
        # --------------------------------------------------
        
    return jsonify({'success': True})

# --- ROTA DE DOWNLOAD (EXCEL) ---

@conferencia_bp.route('/download')
def download_file():
    """
    Gera e baixa o arquivo Excel final com os resultados.
    Cria duas abas: 'Conferencia' (detalhada) e 'Resumo' (por filial).
    """
    if 'data_id' not in session: return redirect(url_for('conferencia.index'))
    
    # Recupera do Disco
    df, forn, sum_df, _ = cache_get(session['data_id'])
    
    # Se o arquivo sumiu do disco (alguém limpou o temp), volta pro início
    if df is None: return redirect(url_for('conferencia.index'))
    
    is_acao = '/acao' in request.referrer or 'acao' in request.url
    
    # Recalcula e gera o resumo baseados nos dados atuais (incluindo edições manuais)
    if is_acao:
        df = calcular_qtd_final_acao(df)
        res, _ = gerar_resumo_acao(df, sum_df)
        nome = f'Conferencia_ACAO_{forn}.xlsx'
    else:
        df = calcular_qtd_final(df)
        res, _ = gerar_resumo_consolidado(df, sum_df)
        nome = f'Conferencia_{forn}.xlsx'

    # Cria o arquivo Excel na memória (BytesIO)
    out = BytesIO()
    with pd.ExcelWriter(out, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Conferencia', index=False)
        res.to_excel(writer, sheet_name='Resumo', index=False)
    
    out.seek(0) # Retorna o ponteiro para o início do arquivo
    return send_file(out, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', as_attachment=True, download_name=nome)

@conferencia_bp.route('/show_export_list')
def show_export_list():
    """
    Rota auxiliar para redirecionar para o download.
    """
    return redirect(url_for('conferencia.download_file'))