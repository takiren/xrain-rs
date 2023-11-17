import polars as pl
import matplotlib.pyplot as plt
import numpy as np
import glob


if __name__ == "__main__":
    data = pl.read_csv("combine.csv", has_header=False).to_numpy()
    fix, ax = plt.subplots()
    im = ax.imshow(data)
    print(data.shape)
    plt.show()
    # np.savetxt("combined.csv", data, delimiter=",", fmt="%d")
