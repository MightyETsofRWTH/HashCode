import os
import numpy as np
import matplotlib.pyplot as plt


class Slice(object):
    def __init__(self, x, y, size_x, size_y):
        self.x, self.y = x, y
        self.size_x, self.size_y = size_x, size_y




if __name__ == '__main__':
    file = os.path.join('datasets', 'small.in')

    with open(file, 'r') as _input:
        R, C, L, H = (int(str(x)) for x in next(_input).strip().split(' '))

        data = list()
        for i in range(R):
            line = str(next(_input)).strip()
            data.append(list(line))

        data_np = np.array(data)

    plt.imshow(data_np == 'M', cmap="Greys", interpolation='nearest')
    plt.show()

    data_usage = np.zeros((R, C))
    data_np_raised = (data_np * 0.5) + 0.5

    for x in range(R):
        for y in range(C):
            if data_usage[x,y]:
                continue
                
            for size_x in range(1, H):
                for size_y in range(1, H / size_x):
                    new_usage = np.zeros((R, C))
                    new_usage[x:x+size_x, y:y+size_y] = True
                    if not np.sum(data_usage * new_usage):
                        new_usage_coverage = data_usage * data_np_raised
                        if np.sum


            # fill data_usage according to chosen slice

    # solution...?
    print()
