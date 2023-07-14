import pandas as pd
import os
import csv

def process_csv(file_path, chunk_size, encoding='utf-8'):
    df_list = []
    reader = pd.read_csv(file_path, sep=';', chunksize=chunk_size, encoding = encoding)

    for chunk in reader:
        df_list.append(chunk)

    df = pd.concat(df_list, ignore_index=True)
    return df

chunk_size = 10000000

exp = process_csv('exp_2022.csv', chunk_size)
exp_mun = process_csv('exp_mun_2022.csv', chunk_size)

dct_urf = process_csv('dct_urf.csv', 100, encoding='latin-1')
dct_urf['NO_URF'] = dct_urf['NO_URF'].str.split(' - ').str[-1].str.replace('IRF ','')

dct_ncm = process_csv('dct_ncm.csv', 100, encoding='latin-1')
dct_uf = process_csv('dct_uf.csv', 100, encoding='latin-1')
dct_uf_mun = process_csv('dct_uf_mun.csv', 100, encoding='latin-1')

cols = set(exp.columns)
dct_cols = set(dct_urf.columns)
list(cols.intersection(dct_cols))

exp = pd.merge(exp, dct_urf, how='left', on=['CO_URF'])
exp = pd.merge(exp, dct_ncm, how='left', on=['CO_UNID', 'CO_NCM'])