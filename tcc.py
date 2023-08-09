import pandas as pd
import os
import csv
import bz2
import sidrapy
import ipeadatapy as ip
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

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
    df = sidrapy.get_table(7435, 
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
    return df

def get_gini():
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
    return df

def pivot_df(df: pd.DataFrame, df_name: str) -> pd.DataFrame:
    df[df_name] = df['value']
    df['cd_uf'] = df['cd_uf'].astype(str)
    piv_df = df[['dt','cd_uf',df_name]]
    return piv_df

def get_pivot_bd(exp, imp, gini, eci):
    piv_exp = pivot_df(exp, 'exp')
    piv_imp = pivot_df(imp, 'imp')
    piv_gini = pivot_df(gini, 'gini')
    piv_eci = pivot_df(eci, 'eci')
    piv_bd = pd.merge(piv_exp, piv_imp, 'left', ['dt','cd_uf'])
    piv_bd = pd.merge(piv_bd, piv_gini, 'left', ['dt','cd_uf'])
    piv_bd = pd.merge(piv_bd, piv_eci, on=['dt','cd_uf'], how='left')
    # piv_bd = piv_bd.query("dt >= '2012-01-01'").reset_index(drop=True)
    piv_bd.to_excel(r"C:\Users\danie\Desktop\TCC\Dados\pivot_bd.xlsx", index=False)
    return piv_bd

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
    return df
