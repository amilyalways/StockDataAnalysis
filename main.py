
from StockDataAnalysis.visualization.Visualization import Visualization

V = Visualization()

fig_para = {
'figsize':(60,35),

}
line_para ={
'linewidth':10,
'color':'r'
}
config = {
    'fig_para':fig_para,
    'line_para':line_para
}

V.trend( "data201303", "201303", 10000, 10000, "abc.png", "C:\\Users\\songxue\\Desktop\\", **config)

