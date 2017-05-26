from matplotlib import pyplot as plt
from matplotlib.ticker import MultipleLocator,FormatStrFormatter
import seaborn as sns


class Figure:
    # set figsize
    def set_figsize(self, figsize):
        plt.figure(figsize=figsize)

    def show(self):
        plt.show()

    # save figure
    def save(self, figname, path, **config):
        plt.savefig(path + figname, **config)
        plt.close()

class LineChart(Figure):

    line_para = {}
    label = {
        'xlabel':"",
        'ylabel':""
    }
    title = ""
    isGrid = True

    def __init__(self,**para):
        if 'line_para' in para:
            self.line_para = para['line_para']
        if 'label' in para:
            self.label = para['label']
        if 'title' in para:
            self.title = para['title']
        if 'isGrid' in para and not para['isGrid']:
            self.isGrid = False
    def set_line_para(self, line_para):
        self.line_para = line_para

    # plot line chart
    def plot_figure(self, x, y):
        plt.plot(y, **self.line_para)
        plt.xticks(range(len(x)), x)
        plt.xlabel(self.label['xlabel'])
        plt.ylabel(self.label['ylabel'])
        plt.title(self.title)
        if self.isGrid:
            plt.grid()

    def plot_figure(self, x,y,linewidth,titlesize,labelsize,xticksize,yticksize):
        plt.plot(y, linewidth=linewidth)
        plt.xticks(range(len(x)), x, rotation=90, fontsize=xticksize)
        plt.yticks(fontsize=yticksize)
        plt.xlabel(self.label['xlabel'], fontsize=labelsize)
        plt.ylabel(self.label['ylabel'], fontsize=labelsize)
        plt.title(self.title, fontsize=titlesize)
        if self.isGrid:
            plt.grid()

class Scatter(Figure):

    def __init__(self):
        pass

    def plot_figure(self, x, y, **config):
        plt.scatter(x, y, **config)


    def legend(self, **config):
        plt.legend(**config)


class MultiFigure(Figure):

    def __init__(self):
        pass

    def plot_figure(self, x, y, ynum, xnum, num, type, **config):

        if 'subplots_adjust' in config:
            plt.subplots_adjust(**config['subplots_adjust'])
        if type == "LineChart":
            figure = LineChart(**config)
        else:                        # type="Scatter"
            figure = Scatter()

        for i in range(1, num):
            plt.subplot(ynum, xnum, i)
            figure.plot_figure(x[i - 1], y[i - 1], **config)

class DoubleAxisLineChart(Figure):


    def __init__(self, ):
        pass



    def plot_first_axis(self,x,y,ylabel,title, xlabel, linewidth,titlesize,labelsize,xticksize,yticksize):
        plt.title(title,fontsize=titlesize)
        plt.xlabel(xlabel,fontsize=labelsize)
        plt.plot(y, linewidth=linewidth)
        plt.xticks(range(len(x)),x,rotation=90,fontsize=xticksize)
        plt.yticks(fontsize=yticksize)
        plt.ylabel(ylabel,fontsize=labelsize)
        plt.grid()



    def plot_second_axis(self, x, y, ylabel,color,linewidth, lablesize,yticksize):
        ax2 = plt.twinx()
        ax2.plot(x, y,color,linewidth=linewidth)
        ax2.set_ylabel(ylabel,fontsize=lablesize)
        plt.yticks(fontsize=yticksize)
        plt.grid()

class BoxFigure(Figure):

    def __init__(self,df):
        self.df = df

    def plot(self):
        self.df.plot(kind='box',figsize=(60,35))

    def sns_plot(self,ymin,ymax):
        plt.subplots(figsize=(60,35))
        plt.xlabel("inMuUpper_outMuUpper",fontsize=55)
        plt.ylabel("Dmax",fontsize=55)
        plt.yticks(range(ymin,ymax),fontsize=45)
        sns.boxplot(self.df, width=0.5)

























'''
line_para = {
    'linewidth':5,
    'color':'r'
}
label = {
    'xlabel': "x",
    'ylabel': "y"
}
config = {
    'line_para':line_para,
    'label':label
}

x = [1,2,3]
y = [1,1,6]
L = LineChart(**config)
L.set_figsize((60,35))
L.plot_figure(x,y)
S = Scatter()
x = [0,1,2]
y = [2,1,5]
S.plot_figure(x,y)
config = {
    'c':'green',
    'marker':'*',
    's':400


}
y = [3,2,6]
S.plot_figure(x,y,**config)
config = {'dpi':100}
S.show()
#S.save("trer.png","C:\\Users\\songxue\\Desktop\\",**config)

'''