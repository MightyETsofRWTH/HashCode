import os
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import progressbar

class Slice(object):
    def __init__(self, x, y, size_x, size_y):
        self.x, self.y = x, y
        self.size_x, self.size_y = size_x, size_y

    def __repr__(self):
        return "(" + str(self.x) + ", " + str(self.y) + "| " + str(self.size_x + 1) + ", " + str(self.size_y + 1) + ")"

    def return_pos(self):
        return str(self.y) + " " + str(self.x) + " " + str(self.y + self.size_y) + " " + str(self.x + self.size_x)

    def extend(self, data_usage, H, L, R, C):

        return data_usage


def check_pos(data_usage, x, y, H, L, R, C, slices):
    for size_x in range(H):
        for size_y in range(int(H / (size_x + 1))):
            if (size_x + 1) * (size_y + 1) > H:
                continue
            new_usage = np.zeros((R, C))
            new_usage[y:y + size_y + 1, x:x + size_x + 1] = True
            if not np.sum(data_usage * new_usage):
                new_usage_coverage = new_usage * data_np_raised
                if np.sum(new_usage_coverage == 0.5) >= L and np.sum(new_usage_coverage == 1) >= L:
                    data_usage += new_usage
                    slices.append(Slice(x, y, size_x, size_y))
                    return data_usage

    return data_usage


if __name__ == '__main__':
    file = os.path.join('datasets', 'medium.in')

    with open(file, 'r') as _input:
        R, C, L, H = (int(str(x)) for x in next(_input).strip().split(' '))
        print('R, C, L, H:', R, C, L, H)

        data = list()
        for i in range(R):
            line = str(next(_input)).strip()
            data.append(list(line))

        data_np = np.array(data)

    fig1 = plt.figure()
    ax1 = fig1.add_subplot(111)
    ax1.imshow(data_np == 'M', cmap="Greys", interpolation='nearest')

    data_usage = np.zeros((R, C))
    data_np_raised = ((data_np == 'M') * 0.5) + 0.5

    slices = []

    bar = progressbar.ProgressBar()
    for x in bar(range(C)):
        for y in range(R):
            if data_usage[y, x]:
                continue

            data_usage = check_pos(data_usage, x, y, H, L, R, C, slices)

    for slice in slices:
        data_usage = slice.extend(data_usage, H, L, R, C)

    # solution...?
    print(len(slices))
    print(slices)
    slice_count = np.sum(data_usage)
    print("Coverage:", slice_count/(R*C))

    for slice in slices:
        ax1.add_patch(patches.Rectangle((slice.x - 0.5, slice.y - 0.5), slice.size_x + 1, slice.size_y + 1, fill=True, edgecolor="#0000FF", facecolor="#0000FF", alpha=0.5))
        if slice.size_x * slice.size_y > H:
            print("Invalid slice!", slice)

    with open("output_medium.txt", 'w') as output_file:
        print("############################################")
        output_file.write(str((int(len(slices)))) + '\n')
        for slice in slices:
            output_file.write(str(slice.return_pos()) + '\n')

    plt.show()
