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
    df = complete_variables(df)
    df = get_additional_variables(df)
    df = get_log_and_product_variables(df)
    export_database(df)

def load_raw_database() -> pd.DataFrame:

    df = pd.read_csv(r"C:\Users\danie\Desktop\TCC\Dados\BASE\basededados_raw.csv", sep = ';')

    return df

def complete_variables(df: pd.DataFrame) -> pd.DataFrame:

    df['gini'] = df['datasus_gini'].fillna(df['pnadc_gini'])
    df['pop'] = df['pop_pnad'].fillna(df['pop_pnadc'])
    df['pop'] = df['pop'].fillna(df['pop_censo'])

    df['pib'] = df['pib'].apply(lambda x:round(x,2))
    df['exp'] = df['exp'].apply(lambda x:round(x,2))
    df['imp'] = df['imp'].apply(lambda x:round(x,2))

    df = df[['dt','sg_uf','exp','imp','gini','pop','pib','eci']]
    df = df.dropna(axis = 0).reset_index(drop=True)

    return df

def get_additional_variables(df: pd.DataFrame) -> pd.DataFrame:

    df['pib_pc'] = df['pib']/df['pop']
    df['pib_pc'] = df['pib_pc'].apply(lambda x:round(x,2))
    df['exp_pib'] = df['exp']/df['pib']
    df['imp_pib'] = df['imp']/df['pib']
    df['trade'] = df['imp']+df['exp']
    df['trade_pib'] = df['trade']/df['pib']

    return df

def get_log_and_product_variables(df: pd.DataFrame) -> pd.DataFrame:

    for col in ['exp','imp','pop','pib','trade']:
        df[f'l{col}'] = df[col].apply(lambda x:math.log(x))

    for cols in [('trade_pib', 'eci')]:
        df[f'{cols[0]}_{cols[1]}'] = df[cols[0]]*df[cols[1]]
        
    return df

def export_database(df: pd.DataFrame) -> None:

    df.to_csv(r"C:\Users\danie\Desktop\TCC\Dados\BASE\basededados.csv", sep = ';', index=False)

def load_database() -> pd.DataFrame:
    
    return pd.read_csv(r"C:\Users\danie\Desktop\TCC\Dados\BASE\basededados.csv", sep = ';')

def get_gini_corr(df: pd.DataFrame) -> float:
    return df.query("pnadc_gini.notna() & datasus_gini.notna()")[['pnadc_gini','datasus_gini']].corr()['pnadc_gini'].iloc[1]

def get_descriptive_stats(df: pd.DataFrame) -> pd.DataFrame:
    desc_stats = df[['gini','eci','trade_pib_eci','trade_pib','lpib','lpop']].describe().T[['count','mean','std','50%','min','max']]
    desc_stats['count'] = desc_stats['count'].apply(lambda x:round(x))
    desc_stats = desc_stats.round(4)
    desc_stats.index = ['Gini','ICE','ICE * Comércio','Comércio','Ln(PIB)','Ln(População)']
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

    # reghdfe gini eci  lpib lpop , a(sg_uf dt) cluster(sg)
    mod1 = PanelOLS.from_formula('gini ~ eci + lpib + lpop + EntityEffects + TimeEffects', data=df)
    res1 = mod1.fit(cov_type='clustered', cluster_entity=True)
    # reghdfe gini trade_pib lpib   lpop , a(sg_uf dt) cluster(sg)
    mod2 = PanelOLS.from_formula('gini ~ trade_pib + lpib + lpop + EntityEffects + TimeEffects', data=df)
    res2 = mod2.fit(cov_type='clustered', cluster_entity=True)
    # reghdfe gini eci  trade_pib lpib   lpop , a(sg_uf dt) cluster(sg)
    mod3 = PanelOLS.from_formula('gini ~ eci + trade_pib + lpib + lpop + EntityEffects + TimeEffects', data=df)
    res3 = mod3.fit(cov_type='clustered', cluster_entity=True)
    # reghdfe gini eci trade_pib_eci trade_pib lpib   lpop , a(sg_uf dt) cluster(sg)
    mod4 = PanelOLS.from_formula('gini ~ eci + trade_pib_eci + trade_pib + lpib + lpop + EntityEffects + TimeEffects', data=df)
    res4 = mod4.fit(cov_type='clustered', cluster_entity=True)

    res_table = pd.DataFrame()
    for r in [res1, res2, res3, res4]:
        r_df = pd.DataFrame([r.params, r.std_errors, r.pvalues])

        r_df['R²'] = r.rsquared

        r_df.columns = pd.Series(r_df.columns).replace({'eci': 'ECI',
                                        'trade_pib_eci': 'Comércio * ECI',
                                        'trade_pib': 'Comércio',
                                        'lpib': 'Ln(PIB)',
                                        'lpop': 'Ln(População)'})
        
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

    res_table = res_table[ [''] + [i for i in ['ECI','Comércio','Comércio * ECI','Ln(PIB)','Ln(População)','R²'] if i in r_df.columns]]
    res_table = res_table.applymap(lambda x:str(x))
    res_table = res_table.applymap(lambda x:'' if x == 'nan' else x)
    res_table = res_table.applymap(lambda x:x.replace('.',','))

    res_table[''] = res_table[''].replace({'parameter': 'Coeficiente',
                    'std_error': 'Erro Padrão',
                    'pvalue': 'Valor-p'})

    res_table.to_csv(r"C:\Users\danie\Desktop\TCC\Dados\tabelas\results_table.csv", sep = ';', encoding='latin1', index = False)
    return res_table
