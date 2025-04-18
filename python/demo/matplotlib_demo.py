import numpy as np
import matplotlib.pyplot as plt
from  matplotlib.image import imread

def draw_graph():
    # 生成数据: 以0.1为单位，生成0到6的数据
    x = np.arange(0, 6, 0.1)
    print(x)
    y1 = np.sin(x)
    print(y1)
    y2 = np.cos(x)
    print(y2)

    # 绘制图形
    plt.plot(x, y1, label="sin")
    plt.plot(x, y2, linestyle="--", label="cos")
    plt.xlabel("x") # x轴标签
    plt.ylabel("y") # y轴标签
    plt.title("sin & cos") #标题
    plt.legend()
    plt.show()

def show_graph():
    img = imread("matplot_demo.png")
    plt.imshow(img)
    plt.show()

if __name__ == '__main__':
    show_graph()