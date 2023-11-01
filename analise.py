import pandas as pd
import statsmodels.api as sm
from statsmodels.formula.api import ols
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import math
from linearmodels.panel import PanelOLS
from linearmodels.panel.results import PanelResults
from tabulate import tabulate

def get_data():
    df = load_raw_database()
    df = get_additional_variables(df)
    df = get_log_and_product_variables(df)
    export_database(df)

def load_raw_database() -> pd.DataFrame:

    df = pd.read_csv(r"C:\Users\danie\Desktop\TCC\Dados\BASE\basededados_raw.csv", sep = ';', decimal = '.', encoding = 'latin1')

    return df

def get_additional_variables(df: pd.DataFrame) -> pd.DataFrame:

    df = df.drop(columns = 'pnad_gini')
    df = df.rename(columns = {'pnadc_gini': 'gini',
                        'pop_pnadc': 'pop',
                        'year': 'dt'})
    df['pib_pc'] = df['pib']/df['pop']
    df['exp_pib'] = df['exp']/df['pib']
    df['imp_pib'] = df['imp']/df['pib']
    df['trade'] = df['imp']+df['exp']
    df['trade_pib'] = df['trade']/df['pib']

    return df

def get_log_and_product_variables(df: pd.DataFrame) -> pd.DataFrame:

    for col in ['exp','imp','pop','pib','trade']:
        df[f'l{col}'] = df[col].apply(lambda x:math.log(x))

    for cols in [('trade_pib', 'eci'),('exp_pib','eci'),('imp_pib','eci')]:
        df[f'{cols[0]}_{cols[1]}'] = df[cols[0]]*df[cols[1]]
        
    return df

def export_database(df: pd.DataFrame) -> None:

    df.to_csv(r"C:\Users\danie\Desktop\TCC\Dados\BASE\basededados.csv", sep = ';', index=False, decimal = '.')

def load_database() -> pd.DataFrame:
    
    return pd.read_csv(r"C:\Users\danie\Desktop\TCC\Dados\BASE\basededados.csv", sep = ';')

def get_gini_corr(df: pd.DataFrame) -> float:
    return df.query("pnadc_gini.notna() & pnad_gini.notna()")[['pnadc_gini','pnad_gini']].corr()['pnadc_gini'].iloc[1]

def get_descriptive_stats(df: pd.DataFrame) -> pd.DataFrame:
    desc_stats = df[['gini','eci','exp_pib','imp_pib','trade_pib','exp_pib_eci','imp_pib_eci','trade_pib_eci','lpib','lpop']].describe().T[['count','mean','std','50%','min','max']]
    desc_stats['count'] = desc_stats['count'].apply(lambda x:round(x))
    desc_stats = desc_stats.round(4)
    desc_stats.index = ['Gini','ICE','Exportações','Importações','Comércio','Exportações * ECI','Importações * ECI','Comércio * ECI','Ln(PIB)','Ln(População)']
    desc_stats.columns = ['Observações','Média','Desvio Padrão','Mediana','Mínimo','Máximo']
    desc_stats = desc_stats.reset_index(names='Variável')
    desc_stats = desc_stats.astype(str)
    for col in desc_stats.columns:
        desc_stats[col] = desc_stats[col].str.replace('.',',',regex=False)
    desc_stats.to_csv(r"C:\Users\danie\Desktop\TCC\Dados\tabelas\desc_stat.csv", index = False, sep = ';', encoding='latin1')
    return desc_stats

def get_results_table(df: pd.DataFrame) -> pd.DataFrame:
    df = df.set_index(['sg_uf', 'dt'])
    df['const'] = 1

    # reghdfe gini eci trade_pib  pib  lpop , a(sg_uf dt) cluster(sg)
    mod1 = PanelOLS.from_formula('gini ~ eci + trade_pib + lpib + lpop + EntityEffects + TimeEffects', data=df)
    res1 = mod1.fit(cov_type='clustered', cluster_entity=True)
    # reghdfe gini eci trade_pib trade_pib_eci pib  lpop , a(sg_uf dt) cluster(sg)
    mod2 = PanelOLS.from_formula('gini ~ eci + trade_pib + trade_pib_eci + lpib + lpop + EntityEffects + TimeEffects', data=df)
    res2 = mod2.fit(cov_type='clustered', cluster_entity=True)
    # reghdfe gini eci exp_pib_eci exp_pib imp_pib_eci imp_pib pib  lpop , a(sg_uf dt) cluster(sg)
    mod3 = PanelOLS.from_formula('gini ~ eci + exp_pib_eci + exp_pib + imp_pib_eci + imp_pib + lpib + lpop + EntityEffects + TimeEffects', data=df)
    res3 = mod3.fit(cov_type='clustered', cluster_entity=True)

    res_table = pd.DataFrame()
    for r in [res1, res2, res3]:
        r_df = pd.DataFrame([r.params, r.std_errors, r.pvalues])

        r_df['R²'] = r.rsquared

        r_df.columns = pd.Series(r_df.columns).replace({'eci': 'ECI',
                                        'trade_pib_eci': 'Comércio * ECI',
                                        'trade_pib': 'Comércio',
                                        'lpib': 'Ln(PIB)',
                                        'lpop': 'Ln(População)',
                                        'exp_pib': 'Exportações',
                                        'imp_pib': 'Importações',
                                        'exp_pib_eci': 'Exportações * ECI',
                                        'imp_pib_eci': 'Importações * ECI'})
        
        r_df = r_df.round(4)
        for col in r_df.columns:
            if col not in ['','R²']:
                r_df.loc['parameter', col] = str(r_df.loc['parameter', col])
                if r_df[col].pvalue < 0.1:
                    r_df.loc['parameter', col] += '*'
                if r_df[col].pvalue < 0.05:
                    r_df.loc['parameter', col] += '*'
                if r_df[col].pvalue < 0.01:
                    r_df.loc['parameter', col] += '*'

        res_table = pd.concat([res_table, r_df])

    res_table = res_table.reset_index(names='')

    cols = [i for i in ['ECI',
            'Exportações',
            'Importações',
            'Comércio',
            'Exportações * ECI',
            'Importações * ECI',
            'Comércio * ECI',
            'Ln(PIB)',
            'Ln(População)',
            'R²'] if i in res_table.columns]
    res_table = res_table[ [''] + cols]
    res_table = res_table.applymap(lambda x:str(x))
    res_table = res_table.applymap(lambda x:'' if x == 'nan' else x)
    res_table = res_table.applymap(lambda x:x.replace('.',','))

    res_table[''] = res_table[''].replace({'parameter': 'Coeficiente',
                    'std_error': 'Erro Padrão',
                    'pvalue': 'Valor-p'})

    res_table = res_table.T
    res_table = res_table.reset_index()
    res_table.to_csv(r"C:\Users\danie\Desktop\TCC\Dados\tabelas\results_table.csv", sep = ';', encoding='latin1', index = False)
    return res_table
