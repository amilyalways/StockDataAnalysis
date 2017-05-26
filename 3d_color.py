from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
from matplotlib import cm
from matplotlib.ticker import LinearLocator, FormatStrFormatter
import numpy as np

'''
fig = plt.figure()
ax = fig.gca(projection='3d')

# Make data.
X = [1,2,3,4,5]
Y = [1,2,3,4,5]
Z = [1,2,1,2,3]
R = [5,4,3,2,1]
# Plot the surface.
surf = ax.plot_surface(X, Y, Z, cmap=cm.coolwarm,
                       linewidth=10, antialiased=False)



# Add a color bar which maps values to colors.
fig.colorbar(surf, shrink=0.5, aspect=5)

plt.show()

'''


x = []
y = []
m = []
n = []
z = []
for i in range(0,360):
    if i % 2 == 0:
        m.append(i)
        n.append(i)
        z.append(str(i))
    else:
        z.append("")
    x.append(i)
    y.append(i)




fig, ax = plt.subplots(figsize=(60,35))
plt.plot(y,linewidth=3)
plt.grid()
plt.xticks(range(len(z)), z)
plt.scatter(m,n,c="red", s=100, marker="o", label="Out & Short")

plt.savefig("C:\\Users\\songxue\\Desktop\\test.png")
