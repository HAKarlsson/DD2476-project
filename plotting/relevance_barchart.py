# a stacked bar plot with errorbars
import numpy as np
import matplotlib.pyplot as plt


N = 10
rel2 = (20, 35, 30, 35, 27, 20, 35, 30, 35, 27)
rel1 = (25, 32, 34, 20, 25, 25, 32, 34, 20, 25)
rel0 = (15, 12, 14, 10, 15, 15, 12, 14, 10, 15)
ind = np.arange(N)    # the x locations for the groups
width = 0.5       # the width of the bars: can also be len(x) sequence

p1 = plt.bar(ind, rel2, width, color='green')
p2 = plt.bar(ind, rel1, width, color="blue", bottom=rel2)
p3 = plt.bar(ind, rel0, width, color="#d62728", bottom=[x+y for x, y in zip(rel1, rel2)])

plt.ylabel('Scores')
plt.title('Scores by group and gender')
plt.xticks(ind, range(1, 11))
plt.yticks(np.arange(0, 100, 10))
plt.legend((p1[0], p2[0], p3[0]), ('Relevance 2', 'Relevance 1', 'Relevance 0'))

plt.show()