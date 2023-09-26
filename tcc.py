import pandas as pd
import os
import numpy as np
import csv
import bz2
import sidrapy
import ipeadatapy as ip
import seaborn as sns
import matplotlib.pyplot as plt
import requests
import numpy as np
import basedosdados as bd

def get_pivot_bd(dfs):
    piv_bd = pd.DataFrame()
    for df in dfs:
        piv = pivot_df(df, df.name)
        if piv_bd.empty:
            piv_bd = piv
        else:
            piv_bd = pd.merge(piv_bd, piv, 'left', ['dt', 'sg_uf'])
    return piv_bd

def process_csv(file_path, chunk_size, encoding='utf-8'):
    df_list = []
    reader = pd.read_csv(file_path, sep=';', chunksize=chunk_size, encoding = encoding)

    for chunk in reader:
        df_list.append(chunk)

    df = pd.concat(df_list, ignore_index=True)
    return df

def process_bz2(file_path):
    with bz2.open(file_path, 'rt') as file:
        df = pd.read_csv(file, delimiter=',')
    return df

def open_dataviva():
    # chunk_size = 10000000
    secex = process_bz2('dataviva_secex_19.csv.bz2')
    rais = process_bz2('rais_19.csv.bz2')
    return secex, rais

def download_gini():
    df = sidrapy.get_table(7435, # Índice de Gini do rendimento domiciliar per capita, a preços médios do ano
                           3, 
                           'all', 
                           variable=10681, 
                           period = 'all')
    df.columns = df.iloc[0]
    df = df.iloc[1:, :]
    df['Valor'] = df['Valor'].astype(float)
    df = df.rename(columns = {"Valor" : "value",
                        "Ano": "dt",
                        "Unidade da Federação (Código)": "cd_uf",
                        "Unidade da Federação": "uf",
                        "Variável": "variable"})
    df['variable'] = 'gini'
    df['dt'] = pd.to_datetime(df['dt'], format = '%Y')
    df = df[['dt', 'cd_uf', 'uf', 'variable','value']]
    df.to_excel(r"C:\Users\danie\Desktop\TCC\Dados\sidra_gini.xlsx", index=False)

def get_pnadc_gini():
    df = pd.read_excel(r"C:\Users\danie\Desktop\TCC\Dados\sidra_gini.xlsx")
    dct = pd.read_csv(r"C:\Users\danie\Desktop\TCC\Dados\dct_uf.csv", delimiter=';')
    dct['cd_uf'] = dct['cd_uf'].astype(str)
    df['cd_uf'] = df['cd_uf'].astype(str)
    df = pd.merge(df,dct,'left',['cd_uf','sg_uf'])
    df = df[['dt','uf','sg_uf','cd_uf','variable','value']]
    df = df.sort_values(['dt','cd_uf']).reset_index(drop=True)
    df.to_excel(r"C:\Users\danie\Desktop\TCC\Dados\sidra_gini.xlsx", index=False)
    df[df['dt'].dt.year == 2012].to_excel(rf"C:\Users\danie\Desktop\TCC\Dados\sidra_gini_12.xlsx",index=False)
    df[df['dt'].dt.year == 2022].to_excel(rf"C:\Users\danie\Desktop\TCC\Dados\sidra_gini_22.xlsx",index=False)
    df = apply_uf_dict(df)
    df['dt'] = df['dt'].astype(str).str[:4]
    df.name = 'pnadc_gini'
    return df

def get_comex(exp_or_imp):
    df = pd.read_csv(rf"C:\Users\danie\Desktop\TCC\Dados\ipeadata_{exp_or_imp}.csv", delimiter=';')
    df.reset_index(inplace=True)
    df.columns = df.iloc[0]
    df = df.iloc[1:, :]
    df = pd.melt(df,id_vars = ['Sigla', 'Código', 'Estado'], var_name='year')
    df.dropna(axis = 0, how = 'any', inplace=True)
    df.columns = ['sg_uf','cd_uf','uf','year','value']
    df['dt'] = pd.to_datetime(df['year'], format = '%Y')
    df['variable'] = exp_or_imp
    df['cd_uf'] = df['cd_uf'].astype(int)
    df = df[['dt','uf','sg_uf','cd_uf','variable','value']]
    df = df.sort_values(['dt','cd_uf'])
    df.to_excel(rf"C:\Users\danie\Desktop\TCC\Dados\ipeadata_{exp_or_imp}.xlsx",index=False)
    df[df['dt'].dt.year == 2012].to_excel(rf"C:\Users\danie\Desktop\TCC\Dados\ipeadata_{exp_or_imp}_12.xlsx",index=False)
    df[df['dt'].dt.year == 2022].to_excel(rf"C:\Users\danie\Desktop\TCC\Dados\ipeadata_{exp_or_imp}_22.xlsx",index=False)
    df = apply_uf_dict(df)
    df['dt'] = df['dt'].astype(str).str[:4]
    df.name = exp_or_imp
    return df

def pivot_df(df: pd.DataFrame, df_name: str) -> pd.DataFrame:
    df[df_name] = df['value']
    piv_df = df[['dt','sg_uf',df_name]]
    return piv_df

def get_secex():
    secex_dir = r'C:\Users\danie\Desktop\TCC\Dados\SECEX\Dados'
    file = os.listdir(secex_dir)[1]
    fdf = pd.DataFrame()
    for file in os.listdir(secex_dir):
        sub_df = pd.read_csv(os.path.join(secex_dir,file), delimiter=';')
        fdf = pd.concat([fdf,sub_df])
    fdf.reset_index(drop=True,inplace=True)
    fdf['sg_uf'] = fdf['Município'].str[-2:]
    df = fdf.groupby(['Ano', 'Codigo SH4', 'sg_uf']).agg({
        'Descrição SH4': 'first',
        'Codigo SH2': 'first',
        'Descrição SH2': 'first',
        'Codigo Seção': 'first',
        'Descrição Seção': 'first',
        'Valor FOB (US$)': 'sum'
    }).reset_index()
    df.columns = ['dt','cd_sh4','sg_uf','sh4','cd_sh2','sh2','cd_sec','sec','value']
    df['dt'] = pd.to_datetime(df['dt'], format='%Y')
    df['dt'] = df['dt'].astype(str).str[:4]
    df = df[['dt','sg_uf','sec','cd_sec','sh2','cd_sh2','sh4','cd_sh4','value']]
    return df

def get_dataviva_eci():
    fdf = pd.DataFrame()
    for file in os.listdir('DATAVIVA'):
        df = pd.read_csv(f'DATAVIVA/{file}')
        fdf = pd.concat([fdf, df]).reset_index(drop=True)
    fdf = fdf[['Ano','ID IBGE','Localidade','Importações','Exportações','Complexidade Econômica','Diversidade de Produtos',
        'Diversidade Efetiva de Produtos','Diversidade de Destino das Exportações','Diversidade Efetiva de Destino das Exportações']]
    return fdf

def fix_eci_magnitudes(df):
    magnitude_dct = {'Bilhões': 1e9,
            'Bilhão': 1e9,
            'Milhões': 1e6,
            'Milhão': 1e6,
            'Mil': 1e3}
    cols = [col for col in df.columns if col not in ['Localidade', 'Ano', 'ID IBGE','Complexidade Econômica']]
    for col in cols:
        mask = df[col].str.contains(' ')

        digits_with_space = df[col][mask].str.split(' ').str[0].astype(float)
        digits_without_space = df[col][~mask].astype(float)

        digits = pd.concat([digits_with_space, digits_without_space])
        digits = digits.sort_index()

        magnitudes_with_space = df[col][mask].str.split(' ').str[1].replace(magnitude_dct)
        magnitudes_without_space = df[col][~mask].astype(float)/df[col][~mask].astype(float)

        magnitudes = pd.concat([magnitudes_with_space, magnitudes_without_space])
        magnitudes = magnitudes.sort_index()

        df[col] = digits * magnitudes
    return df

def get_uf_dct():
    dct = pd.read_csv(r"C:\Users\danie\Desktop\TCC\Dados\secex_dct_uf.csv", encoding='latin-1', delimiter = ';')
    dct = dct.rename(columns = {'CO_UF': 'cd_uf',
                                'SG_UF': 'sg_uf',
                                'NO_UF': 'uf',
                                'NO_REGIAO': 'nm_reg'})
    return dct

def get_dataviva():
    df = get_dataviva_eci()
    df.loc[df['ID IBGE'] == '-', 'ID IBGE'] = 0

    df = df.applymap(lambda x: str(x))
    df = df.applymap(lambda x: x.replace('USD ', ''))
    df = df.applymap(lambda x: x.replace(',', '.'))
    df = fix_eci_magnitudes(df)
    df['nm_variable'] = 'eci'
    df = df[['Ano','Localidade','ID IBGE','nm_variable','Complexidade Econômica']]
    df = df.rename(columns = {'Ano': 'dt', 
                    'Localidade': 'uf', 
                    'ID IBGE': 'cd_uf',
                    'Complexidade Econômica': 'value'})
    df['dt'] = pd.to_datetime(df['dt'], format = '%Y')
    df['value'] = df['value'].astype(float)
    df = apply_uf_dict(df)
    df['dt'] = df['dt'].astype(str).str[:4]
    df.name = 'eci'
    return df

def download_pop_pnadc():
    pop_pnadc = sidrapy.get_table(7436, # População residente PNADc
                        3, 
                        'all', 
                        period = 'all',
                        variable = 606)
    pop_pnadc = pop_pnadc.iloc[1:, :]
    pop_pnadc = pop_pnadc[['V','D1C','D2C']]
    pop_pnadc = pop_pnadc.rename(columns = {'V': 'value',
                                'D1C': 'cd_uf',
                                'D2C': 'dt'})
    pop_pnadc['value'] = pop_pnadc['value'].astype(int)*1000
    pop_pnadc['variable'] = 'pop_pnadc'
    pop_pnadc = pop_pnadc[['dt','cd_uf','variable','value']]
    pop_pnadc.to_csv(r"C:\Users\danie\Desktop\TCC\Dados\SIDRA\pop_pnadc.csv", index=False)

def download_pop_pnad():
    pop_pnad = sidrapy.get_table(261, # População residente PNAD
                    3, 
                    'all', 
                    period = 'all',
                    variable = 93)
    pop_pnad = pop_pnad.iloc[1:, :]
    pop_pnad = pop_pnad[['V','D1C','D2C']]
    pop_pnad = pop_pnad.rename(columns = {'V': 'value',
                                'D1C': 'cd_uf',
                                'D2C': 'dt'})
    pop_pnad['value'] = pop_pnad['value'].astype(int)*1000
    pop_pnad['variable'] = 'pop_pnad'
    pop_pnad = pop_pnad[['dt','cd_uf','variable','value']]
    pop_pnad.to_csv(r"C:\Users\danie\Desktop\TCC\Dados\SIDRA\pop_pnad.csv", index=False)

def get_pop_pnadc():
    pop_pnadc = pd.read_csv(r"C:\Users\danie\Desktop\TCC\Dados\SIDRA\pop_pnadc.csv")
    pop_pnadc['cd_uf'] = pop_pnadc['cd_uf'].astype(str)
    pop_pnadc = apply_uf_dict(pop_pnadc)
    pop_pnadc['dt'] = pop_pnadc['dt'].astype(str)
    pop_pnadc.name = 'pop_pnadc'
    return pop_pnadc

def get_pop_pnad():
    pop_pnad = pd.read_csv(r"C:\Users\danie\Desktop\TCC\Dados\SIDRA\pop_pnad.csv")
    pop_pnad['cd_uf'] = pop_pnad['cd_uf'].astype(str)
    pop_pnad = apply_uf_dict(pop_pnad)
    pop_pnad['dt'] = pop_pnad['dt'].astype(str)
    pop_pnad.name = 'pop_pnad'
    return pop_pnad

def download_pib():
    df = ip.timeseries('PIBPMCE',
              yearGreaterThan = 1900)
    df.to_csv(r"C:\Users\danie\Desktop\TCC\Dados\IPEADATA\pib_uf.csv")

def get_pib():
    df = pd.read_csv(r"C:\Users\danie\Desktop\TCC\Dados\IPEADATA\pib_uf.csv")
    df.reset_index(inplace=True)
    df['variable'] = 'pib'
    df = df[['DATE', 'TERCODIGO', 'variable','VALUE (R$ (mil))']]
    df.columns = ['dt', 'cd_uf', 'variable','value']
    df['dt'] = pd.to_datetime(df['dt'], format = '%Y-%m-%d')
    df['cd_uf'] = df['cd_uf'].astype(str)
    df['value'] = df['value']*1000
    df = df[(df['dt'].dt.year >= 1997) & (df['cd_uf'].apply(lambda x:len(x)) == 2)]
    df.sort_values(['cd_uf','dt'], inplace=True)
    df = apply_uf_dict(df)
    df['dt'] = df['dt'].astype(str).str[:4]
    df.name = 'pib'
    return df

def download_income_share():
    df = sidrapy.get_table(7545, # Rendimento médio mensal real das pessoas de 14 anos ou mais de idade ocupadas na semana de referência com rendimento de trabalho, efetivamente recebido em todos os trabalhos, a preços médios do ano, por classes simples de percentual das pessoas em ordem crescente de rendimento efetivamente recebido
                    3, 
                    'all', 
                    period = 'all',
                    variable = 10850,
                    classification='1046/all')

    df.to_csv(r"C:\Users\danie\Desktop\TCC\Dados\SIDRA\income_share.csv", index=False)

def get_income_share():
    df = pd.read_csv(r"C:\Users\danie\Desktop\TCC\Dados\SIDRA\income_share.csv")
    df = df.iloc[1:, :]
    df = df[['V','D1C','D2C','D4N']]
    df = df.rename(columns = {'V': 'value',
                            'D1C': 'cd_uf',
                            'D2C': 'dt',
                            'D4N': 'percentile'})
    df = df.drop(index = df[df['value'] == '-'].index).reset_index(drop=True)
    df['value'] = df['value'].astype(int)
    df['dt'] = pd.to_datetime(df['dt'], format = '%Y')
    df['variable'] = 'income_share'
    df = df[['dt','cd_uf','variable','percentile','value']]
    df = apply_uf_dict(df)
    df['dt'] = df['dt'].astype(str).str[:4]
    df.name = 'income_share'
    return df

def get_uf_cd_uf_dict():
    df = pd.read_csv(r"C:\Users\danie\Desktop\TCC\Dados\SIDRA\uf_cd_uf_dict.csv")
    # df = df[['D1N','D1C']].iloc[1:,:].drop_duplicates()
    # df = df.rename(columns = {'D1N': 'uf',
    #                         'D1C': 'cd_uf'}).reset_index(drop=True)
    # uf = ['RO', 'AC', 'AM', 'RR', 'PA', 'AP', 'TO', 'MA', 'PI', 'CE', 'RN', 'PB', 'PE', 'AL', 'SE', 'BA', 'MG', 'ES', 'RJ', 'SP', 'PR', 'SC', 'RS', 'MS', 'MT', 'GO', 'DF']
    # df['sg_uf'] = uf
    df['cd_uf'] = df['cd_uf'].astype(str)
    dct = dict(zip(df['cd_uf'], df['sg_uf']))
    dct.update(dict(zip(df['uf'], df['sg_uf'])))
    return dct

def get_datasus_gini():
    df = pd.read_csv(r"C:\Users\danie\Desktop\TCC\Dados\DATASUS\gini.csv", encoding='latin1', delimiter=';') # Índice de Gini da renda domiciliar per capita http://tabnet.datasus.gov.br/cgi/idb2011/b09ufa.htm
    ano = df.iloc[2,1:].str.split(' - ').str[0]
    df = df.iloc[3:36,:]
    df.columns = ['uf'] + list(ano)
    df = pd.melt(df, id_vars = ['uf'], value_name='value', var_name='dt')
    df.loc[df.dt.isin(['1991', '2000', '2010']), 'fonte'] = 'censo'
    df.loc[~df.dt.isin(['1991', '2000', '2010']), 'fonte'] = 'pnad'
    df['variable'] = 'gini'
    df = apply_uf_dict(df)
    df['dt'] = df['dt'].astype(str).str[:4]
    df['value'] = df['value'].str.replace(',','.',regex=False).astype(float)
    df = df[['dt','fonte','sg_uf','variable','value']]
    df.name = 'datasus_gini'
    return df

def apply_uf_dict(df):
    dct = get_uf_cd_uf_dict()
    if 'uf' in df.columns:
        df['sg_uf'] = df['uf'].replace(dct)
    elif 'cd_uf' in df.columns:
        df['sg_uf'] = df['cd_uf'].replace(dct)
    df = df.loc[df.sg_uf.apply(lambda x:len(x) == 2)]
    return df

def download_pnad_household_income():
    query = """
    SELECT ano, sigla_uf, renda_mensal_domiciliar_compativel_1992 
    FROM `basedosdados.br_ibge_pnad.microdados_compatibilizados_domicilio` 
    """
    df = bd.read_sql(query, billing_project_id="rais-357517")
    df = df.sort_values(['ano','sigla_uf'])
    df.columns = ['dt','sg_uf','renda_mensal_domiciliar']
    df.to_csv(r'C:\Users\danie\Desktop\TCC\Dados\BASEDOSDADOS\pnad_renda_mensal_domicilar_uf_1981_2015.csv', index=False)

def download_pndac_household_income():
    query = """
    SELECT ano, sigla_uf, VD4020 
    FROM `basedosdados.br_ibge_pnadc.microdados` 
    WHERE trimestre = 1 AND VD4020 IS NOT NULL
    """
    df = bd.read_sql(query, billing_project_id="rais-357517") # Rendimento mensal efetivo de todos os trabalhos para pessoas de 14 anos ou mais de idade (apenas para pessoas que receberam em dinheiro, produtos ou mercadorias em qualquer trabalho)	
    df = df.sort_values(['ano','sigla_uf'])
    df.columns = ['dt','sg_uf','renda_mensal_efetiva']
    df['dt'] = df['dt'].astype(str)
    df.to_csv(r'C:\Users\danie\Desktop\TCC\Dados\BASEDOSDADOS\pnadc_renda_mensal_efetiva_uf_2012_2023.csv', index=False)

def calculate_gini(x):
    n = len(x)
    x_sorted = np.sort(x)
    cum_income = np.cumsum(x_sorted)
    cumulative_percentage = cum_income / cum_income[-1]
    return 1 - (2 * np.sum(cumulative_percentage) / n) + (1 / n)

def get_microdados_pnad_gini():
    df = pd.read_csv(r'C:\Users\danie\Desktop\TCC\Dados\BASEDOSDADOS\pnad_renda_mensal_domicilar_uf_1981_2015.csv')
    gini_results = []
    grouped = df[df['renda_mensal_domiciliar'].notna()].groupby(['sg_uf', 'dt'])
    for (uf, dt), group in grouped:
        income_array = group['renda_mensal_domiciliar'].values
        gini_index = calculate_gini(income_array)
        gini_results.append({'dt': dt, 'sg_uf': uf, 'gini': gini_index})
    gini_df = pd.DataFrame(gini_results)
    gini_df = gini_df.sort_values(['dt','sg_uf']).reset_index(drop=True)
    gini_df = gini_df.rename(columns = {'gini': 'value'})
    gini_df['variable'] = 'microdados_pnad_gini'
    gini_df = gini_df[['dt','sg_uf','variable','value']]
    gini_df['dt'] = gini_df['dt'].astype(str).str[:4]
    gini_df.name = 'microdados_pnad_gini'
    return gini_df

def get_microdados_pnadc_gini():
    df = pd.read_csv(r'C:\Users\danie\Desktop\TCC\Dados\BASEDOSDADOS\pnadc_renda_mensal_efetiva_uf_2012_2023.csv')
    gini_results = []
    grouped = df[df['renda_mensal_efetiva'].notna()].groupby(['sg_uf', 'dt'])
    for (uf, dt), group in grouped:
        income_array = group['renda_mensal_efetiva'].values
        gini_index = calculate_gini(income_array)
        gini_results.append({'dt': dt, 'sg_uf': uf, 'gini': gini_index})
    gini_df = pd.DataFrame(gini_results)
    gini_df = gini_df.sort_values(['dt','sg_uf']).reset_index(drop=True)
    gini_df = gini_df.rename(columns = {'gini': 'value'})
    gini_df['variable'] = 'microdados_pnadc_gini'
    gini_df = gini_df[['dt','sg_uf','variable','value']]
    gini_df['dt'] = gini_df['dt'].astype(str).str[:4]
    gini_df.name = 'microdados_pnadc_gini'
    return gini_df

def download_gini_rend_med():
    df = sidrapy.get_table(7453, # Índice de Gini do rendimento médio mensal real das pessoas de 14 anos ou mais de idade ocupadas na semana de referência com rendimento de trabalho, habitualmente recebido em todos os trabalhos, a preços médios do ano
                            3, 
                            'all', 
                            variable=10806, 
                            period = 'all')
    df.columns = df.iloc[0]
    df = df.iloc[1:, :]
    df['Valor'] = df['Valor'].astype(float)
    df['Ano'] = df['Ano'].astype(str)
    df = df.rename(columns = {"Valor" : "value",
                        "Ano": "dt",
                        "Unidade da Federação (Código)": "cd_uf",
                        "Unidade da Federação": "uf",
                        "Variável": "variable"})
    df['variable'] = 'gini'
    df = df[['dt', 'cd_uf', 'uf', 'variable','value']]
    df.to_csv(r'C:\Users\danie\Desktop\TCC\Dados\SIDRA\gini_rendimento_medio_mensal_real.csv')

def get_gini_rend_med():
    df = pd.read_csv(r'C:\Users\danie\Desktop\TCC\Dados\SIDRA\gini_rendimento_medio_mensal_real.csv', index_col=0)
    df = apply_uf_dict(df)
    df['dt'] = df['dt'].astype(str)
    df.name = 'gini_rend_med'
    return df

def download_pop_censo():
    pop_censo = sidrapy.get_table(136, 3, 'all', period= 'all')
    pop_censo = pop_censo.iloc[1:, :]
    pop_censo = pop_censo[['V','D1C','D2C']]
    pop_censo = pop_censo.rename(columns = {'V': 'value',
                                'D1C': 'cd_uf',
                                'D2C': 'dt'})
    pop_censo['value'] = pop_censo['value'].astype(int)
    pop_censo['variable'] = 'pop_censo'
    pop_censo = pop_censo[['dt','cd_uf','variable','value']]
    pop_censo.to_csv(r"C:\Users\danie\Desktop\TCC\Dados\SIDRA\pop_censo.csv", index=False, sep = ';')

def get_pop_censo():
    pop_censo = pd.read_csv(r"C:\Users\danie\Desktop\TCC\Dados\SIDRA\pop_censo.csv", sep = ';')
    pop_censo['cd_uf'] = pop_censo['cd_uf'].astype(str)
    pop_censo = apply_uf_dict(pop_censo)
    pop_censo['dt'] = pop_censo['dt'].astype(str)
    pop_censo.name = 'pop_censo'
    return pop_censo

def get_data():
    exp = get_comex('exp')
    imp = get_comex('imp')
    pnadc_gini = get_pnadc_gini()
    microdados_pnad_gini = get_microdados_pnad_gini()
    microdados_pnadc_gini = get_microdados_pnadc_gini()
    datasus_gini = get_datasus_gini()
    gini_rend_med = get_gini_rend_med()
    eci = get_dataviva()
    pop_pnadc = get_pop_pnadc()
    pop_pnad = get_pop_pnad()
    pop_censo = get_pop_censo()
    pib = get_pib()

    dfs = [exp, imp, pnadc_gini, microdados_pnad_gini, microdados_pnadc_gini, datasus_gini, gini_rend_med, eci, pop_pnadc, pop_pnad, pop_censo, pib]
    dados = get_pivot_bd(dfs)
    dados.to_csv(r"C:\Users\danie\Desktop\TCC\Dados\BASE\basededados_raw.csv", sep = ';', index = False)
