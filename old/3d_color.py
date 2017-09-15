
import matplotlib.pyplot as plt
import numpy as np


x = np.random.rand(10)
print x
fig = plt.figure()
ax = fig.add_subplot(2, 1, 1)
ax.plot(x, 'ko--')
ax.set_xticks([0, 0.1, 0.3, 1])

ax2 = fig.add_subplot(2, 1, 2)
ax2.plot(x+1, 'ko', label="a")
ax2.plot(x+2, label="b")
ax2.legend(loc='best')
plt.show()