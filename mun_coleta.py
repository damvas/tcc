import pandas as pd

def mun():
    df = pd.read_csv(r'C:\Users\danie\Desktop\TCC\Dados\ADH\mun_2010.csv', sep = ';', encoding = 'latin1', decimal = ',')
    df['pmpob'] = df['pmpob']/100

    pib = get_pib()
    df['id'] = df['id'].astype(str)
    df = pd.merge(df, pib, 'left', 'id')
    df = df.drop(columns = 'mun')

    eci = get_eci()
    df = pd.merge(df, eci, 'left', 'id')
    df = df.drop(columns = 'mun')

    cambio = get_exchange_rate()
    exp = get_exp()
    exp = convert_exp(exp, cambio)
    df = pd.merge(df,exp,'left',['municipio','sg_uf'])

    imp = get_imp()
    imp = convert_imp(imp, cambio)
    df = pd.merge(df,imp,'left',['municipio','sg_uf'])

    df = df.reset_index(drop=True)
    df = df.rename(columns = {'exp_brl': 'exp', 'imp_brl': 'imp', 'pib_brl': 'pib', 'pesotot': 'pop'})
    df['exp'] = df['exp'].fillna(0)
    df['imp'] = df['imp'].fillna(0)
    df = df.query("eci.notna()").reset_index(drop=True)

    df = df[['sg_uf','uf','municipio','id'] + [col for col in df.columns if col not in ['id','uf','sg_uf','municipio']]]
    df.to_csv(r'C:\Users\danie\Desktop\TCC\Dados\BASE\base_mun_raw.csv', index = False, sep = ';', decimal = ',', encoding = 'latin1')

def get_data():
    df = pd.read_csv(r'C:\Users\danie\Desktop\TCC\Dados\ADH\mun_2010.csv', sep = ';', encoding = 'latin1', decimal = ',')
    df['pmpob'] = df['pmpob']/100

    pib = get_pib()
    df['id'] = df['id'].astype(str)
    df = pd.merge(df, pib, 'left', 'id')
    df = df.drop(columns = 'mun')

    eci = get_eci()
    df = pd.merge(df, eci, 'left', 'id')
    df = df.drop(columns = 'mun')

    cambio = get_exchange_rate()
    exp = get_exp()
    exp = convert_exp(exp, cambio)
    df = pd.merge(df,exp,'left',['municipio','sg_uf'])

    imp = get_imp()
    imp = convert_imp(imp, cambio)
    df = pd.merge(df,imp,'left',['municipio','sg_uf'])

    df = df.reset_index(drop=True)
    df = df.rename(columns = {'exp_brl': 'exp', 'imp_brl': 'imp', 'pib_brl': 'pib', 'pesotot': 'pop'})
    df['exp'] = df['exp'].fillna(0)
    df['imp'] = df['imp'].fillna(0)
    df = df.query("eci.notna()").reset_index(drop=True)

    df = df[['sg_uf','uf','municipio','id'] + [col for col in df.columns if col not in ['id','uf','sg_uf','municipio']]]
    df.to_csv(r'C:\Users\danie\Desktop\TCC\Dados\BASE\base_mun_raw.csv', index = False, sep = ';', decimal = ',', encoding = 'latin1')

def download_adh_mun_2010():
    df = pd.read_excel(r"C:\Users\danie\Desktop\TCC\Dados\ADH\Atlas 2013_municipal, estadual e Brasil.xlsx", sheet_name='MUN 91-00-10')
    df = df.query("ANO == 2010")
    df = df[['Codmun7','UF','Município','GINI','PMPOB','RDPC','R2040','R1040','THEIL','E_ANOSESTUDO','pesotot']]
    df.columns = ['id','uf','mun','gini','pmpob','rdpc','r2040','r1040','theil','e_anosestudo','pesotot']
    df.to_csv(r'C:\Users\danie\Desktop\TCC\Dados\ADH\mun_2010.csv', sep = ';', index = False, encoding = 'latin1', decimal = ',')

def get_pib():
    pib = pd.read_csv(r"C:\Users\danie\Desktop\TCC\Dados\IPEADATA\pib_mun_2010.csv", sep = ';')
    pib = pib.reset_index(names = ['sg_uf','id','municipio','pib_brl'])
    pib = pib.dropna(axis = 1, how = 'all')
    pib = pib.drop(index = 0).reset_index(drop=True)
    pib['pib_brl'] = pib['pib_brl'].str.replace(",",".",regex=False)
    pib['pib_brl'] = pib['pib_brl'].astype(float)
    pib['pib_brl'] = pib['pib_brl']*1000
    return pib

def get_exchange_rate():
    cambio = pd.read_csv(r"C:\Users\danie\Desktop\TCC\Dados\YAHOO\cambio.csv")
    cambio['Date'] = pd.to_datetime(cambio['Date'], format = '%Y-%m-%d')
    cambio['year'] = cambio['Date'].dt.year
    cambio['month'] = cambio['Date'].dt.month
    cambio_meses = cambio.groupby(['year','month'])['Adj Close'].mean().reset_index()
    cambio_meses.columns = ['year','month','brl_usd']
    cambio_meses['year'] = cambio_meses['year'].astype(str)
    cambio_meses['month'] = cambio_meses['month'].astype(str)
    cambio_meses['usd_brl'] = 1/cambio_meses['brl_usd']
    return cambio_meses

def get_eci():
    eci = pd.read_csv(r"C:\Users\danie\Desktop\TCC\Dados\DATAVIVA\MUN\eci_mun_2010.csv", sep = ',')
    eci = eci[['ID IBGE', 'Localidade', 'ICE-R - Comércio']]
    eci.columns = ['id','mun','eci']
    eci['id'] = eci['id'].astype(str)
    eci['eci'] = eci['eci'].str.replace(',','.',regex=False)
    eci['eci'] = eci['eci'].astype(float)
    return eci

def get_exp():
    exp = pd.read_csv(r"C:\Users\danie\Desktop\TCC\Dados\SECEX\exp_2010_by_month.csv", sep = ';', encoding = 'utf-8')
    exp.columns = ['year','month', 'municipio', 'uf', 'exp_usd']
    exp = exp[['year','month','municipio','exp_usd']]
    split = exp['municipio'].str.split(' - ')
    exp['municipio'] = split.str[0]
    exp['sg_uf'] = split.str[1]
    exp['year'] = exp['year'].astype(str)
    exp['month'] = exp['month'].astype(str)
    return exp

def get_imp():
    imp = pd.read_csv(r"C:\Users\danie\Desktop\TCC\Dados\SECEX\imp_2010_by_month.csv", sep = ';', encoding = 'utf-8')
    imp.columns = ['year','month', 'municipio', 'uf', 'imp_usd']
    imp = imp[['year','month','municipio','imp_usd']]
    split = imp['municipio'].str.split(' - ')
    imp['municipio'] = split.str[0]
    imp['sg_uf'] = split.str[1]
    imp['year'] = imp['year'].astype(str)
    imp['month'] = imp['month'].astype(str)
    return imp

def convert_exp(exp: pd.DataFrame, cambio: pd.DataFrame) -> pd.DataFrame:
    exp = pd.merge(exp, cambio, 'left', ['year','month'])
    exp['exp_brl'] = exp['exp_usd']*exp['brl_usd']
    exp = exp.groupby(['municipio','sg_uf'])['exp_brl'].sum().reset_index()
    return exp

def convert_imp(imp: pd.DataFrame, cambio: pd.DataFrame) -> pd.DataFrame:
    imp = pd.merge(imp, cambio, 'left', ['year','month'])
    imp['imp_brl'] = imp['imp_usd']*imp['brl_usd']
    imp = imp.groupby(['municipio','sg_uf'])['imp_brl'].sum().reset_index()
    return imp
