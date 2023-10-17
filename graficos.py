import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt

def get_charts():
    uf = get_dataframe()
    for variable in ['eci', 'gini', 'trade_pib']:
        for dt in [2006, 2020]:
            fig = get_fig(uf, dt, variable)
            save_fig(fig, variable, dt)

def load_database() -> pd.DataFrame:
    
    return pd.read_csv(r"C:\Users\danie\Desktop\TCC\Dados\BASE\basededados.csv", sep = ';')

def get_fig(uf: pd.DataFrame, dt: int, col: str):
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

    # plt.title('Índice de Complexidade Econômica (ICE)', fontsize=16)

    plt.close()
    return fig

def save_fig(fig, col: str, dt: int):
    fig.savefig(rf'C:\Users\danie\Desktop\TCC\Dados\graficos\{col}_{dt}.png', format='png', dpi=300, transparent = True, bbox_inches='tight')

def get_dataframe():
    gdf = gpd.read_file(r"C:\Users\danie\Desktop\Biomas\MAP\bra_uf.json")
    df = load_database()
    dct = pd.read_csv(r'C:\Users\danie\Desktop\TCC\Dados\SIDRA\uf_cd_uf_dict.csv')
    dct = dict(zip(dct['sg_uf'], dct['cd_uf']))
    df = df.rename(columns = {'sg_uf': 'id'})
    df['id'] = df['id'].replace(dct)
    uf = pd.merge(gdf, df, 'left', 'id')
    return uf
