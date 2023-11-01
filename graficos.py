import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np
from matplotlib.colors import SymLogNorm
from matplotlib.colors import LinearSegmentedColormap

def get_charts():
    uf = get_uf_dataframe()
    for variable in ['eci', 'gini', 'trade_pib']:
        for dt in [2006, 2020]:
            fig = get_uf_fig(uf, dt, variable)
            save_uf_fig(fig, variable, dt)

def load_uf_database() -> pd.DataFrame:
    
    return pd.read_csv(r"C:\Users\danie\Desktop\TCC\Dados\BASE\basededados.csv", sep = ';')

def load_mun_database() -> pd.DataFrame:
    
    return pd.read_csv(r"C:\Users\danie\Desktop\TCC\Dados\BASE\base_mun.csv", sep = ';', decimal = '.', encoding = 'latin1')

def get_uf_fig(uf: pd.DataFrame, dt: int, col: str):
    fig,ax = plt.subplots(figsize=(12,10))

    data = uf.query(f"dt == {dt}")

    if col == 'gini':
        data.plot(ax=ax, column=col, legend=False, cmap='coolwarm', vmin = uf[col].min(), vmax = uf[col].max())
    else:
        data.plot(ax=ax, column=col, legend=False, cmap='coolwarm_r', vmin = uf[col].min(), vmax = uf[col].max())

    for x, y, label in zip(data.geometry.centroid.x, data.geometry.centroid.y, data[col]):
        ax.annotate(text=f'{label:.2f}', xy=(x-1, y), xytext=(3, 3), textcoords='offset points', fontsize=9, color='black')

    ax.set_axis_off()

    xmin, ymin, xmax, ymax = uf.total_bounds
    ax.set_xlim([xmin, xmax])
    ax.set_ylim([ymin, ymax])  

    plt.close()
    return fig

def get_mun_fig(df: pd.DataFrame, uf_gdf, col: str):
    fig,ax = plt.subplots(figsize=(12,10))

    colors = [(0.1, 0.1, 0.6), (0.8, 0.3, 0.8)]
    colors_r = [(0.8, 0.3, 0.8), (0.1, 0.1, 0.6)]
    n_bins = 100 
    cmap_name = "custom_diverging"

    cm = LinearSegmentedColormap.from_list(cmap_name, colors, N=n_bins)
    cm_r = LinearSegmentedColormap.from_list(cmap_name, colors_r, N=n_bins)

    uf_gdf.boundary.plot(ax=ax, color='black', linewidth=0.8)

    vmin = max(df[col].min(), 0.01)
    vmax = df[col].max()

    linthresh = 0.01 
    norm = SymLogNorm(linthresh=linthresh, vmin=df[col].min(), vmax=df[col].max())

    if col == 'gini':
        df.plot(ax=ax, column=col, legend=True, cmap=cm, 
                vmin = vmin, vmax = vmax, 
                missing_kwds={"color": "white"})
    else:
        df.plot(ax=ax, column=col, legend=True, cmap=cm_r, 
                vmin = vmin, vmax = vmax,
                norm = norm, 
                missing_kwds={"color": "white"})
    ax.set_axis_off()

    xmin, ymin, xmax, ymax = df.total_bounds
    ax.set_xlim([xmin, xmax])
    ax.set_ylim([ymin, ymax])  

    plt.close()
    return fig

def save_uf_fig(fig, col: str, dt: int):
    fig.savefig(rf'C:\Users\danie\Desktop\TCC\Dados\graficos\{col}_{dt}.png', format='png', dpi=300, transparent = True, bbox_inches='tight')

def save_mun_fig(fig, col: str):
    fig.savefig(rf'C:\Users\danie\Desktop\TCC\Dados\graficos\mun_{col}.png', format='png', dpi=500, transparent = True, bbox_inches='tight')

def get_uf_dataframe():
    gdf = gpd.read_file(r"C:\Users\danie\Desktop\Biomas\MAP\bra_uf.json")
    df = load_uf_database()
    dct = pd.read_csv(r'C:\Users\danie\Desktop\TCC\Dados\SIDRA\uf_cd_uf_dict.csv')
    dct = dict(zip(dct['sg_uf'], dct['cd_uf']))
    df = df.rename(columns = {'sg_uf': 'id'})
    df['id'] = df['id'].replace(dct)
    uf = pd.merge(gdf, df, 'left', 'id')
    return uf
