import pandas as pd
import statsmodels.api as sm
from statsmodels.formula.api import ols
import matplotlib.pyplot as plt
import math
from linearmodels.panel import PanelOLS
from linearmodels.panel.results import PanelResults

df = pd.read_csv(r"C:\Users\danie\Desktop\TCC\Dados\BASE\basededados_raw.csv")

df['gini'] = df['datasus_gini'].fillna(df['pnadc_gini'])
df['pop'] = df['pop_pnad'].fillna(df['pop_pnadc'])
df = df[['dt','sg_uf','exp','imp','gini','pop','pib','eci']]
df = df.dropna(axis = 0).reset_index(drop=True)

df['pib_pc'] = df['pib']/df['pop']
df['exp_pib'] = df['exp']/df['pib']
df['imp_pib'] = df['imp']/df['pib']
df['trade'] = df['imp']+df['exp']
df['trade_pib'] = df['trade']/df['pib']

for col in ['exp','imp','pop','pib']:
    df[f'l{col}'] = df[col].apply(lambda x:math.log(x))

for cols in [('lexp','eci'), ('limp','eci'), ('trade','eci')]:
    df[f'{cols[0]}_{cols[1]}'] = df[cols[0]]*df[cols[1]]

df.to_csv(r"C:\Users\danie\Desktop\TCC\Dados\BASE\basededados.csv", index=False)