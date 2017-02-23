import os
import numpy as np
import matplotlib.pyplot as plt

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

    # solution...?
    print()
