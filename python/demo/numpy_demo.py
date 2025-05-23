import numpy as np


def one_dimension_arr():
    # -------------- 一维数组 --------------
    x = np.array([1.0, 2.0, 3.0])
    print(x)

    # numpy的算数运算，这里是对应元素的运算element-wise，即数组和数组的运算
    x = np.array([1.0, 2.0, 3.0])
    y = np.array([2.0, 4.0, 6.0])
    print(x + y)
    print(x - y)
    print(x * y)
    print(x / y)

    # 数组和标量的运算，这个功能也被称为广播，广播的元素是标量？
    print(x / 2)

def n_dimension_arr():
    # -------------- N维数组 --------------
    A = np.array([[1,2], [3,4]])
    print(A)
    # 矩阵A的形状可以通过shape查看
    print(A.shape)
    # 矩阵元素的数据类型可以通过dtype查看
    print(A.dtype)

    B = np.array([[3,0], [0,6]])
    print(A+B)
    print(A*B)

    print(A * 10)

def get_element():
    X = np.array([[51, 55], [14, 19], [0, 4]])
    print(X)
    print(f"X[0]: {X[0]}")
    print(f"X[0][1] = {X[0][1]}")

    for row in X:
        print(f"row: {row}")

    # 转换为一维数组
    X = X.flatten()
    print(X)
    # 访问0、2、4的元素
    print(X[np.array([0,2,4])])
    print(X[[0,2,4]])

    # 获取满足一定条件的数据
    print(X > 15)
    print(X[X>15])

def dot_two_arr():
    arr_x = np.array([1, 2])
    print(arr_x.shape)
    print(arr_x.ndim)

    arr_w = np.array([[1, 3, 5], [2, 4, 6]])
    print(arr_w.shape)
    print(arr_w.ndim)

    arr_y = np.dot(arr_x, arr_w)


def sigmoid(x):
    return 1 / (1 + np.exp(-x))

def test_sigmoid():
    x = np.array([-1.0, 1.0, 2.0])
    print(sigmoid(x))

def softmax(a):
    c = np.max(a)
    exp_a = np.exp(a - c) # 溢出策略
    sum_exp_a = np.sum(exp_a)
    y = exp_a / sum_exp_a
    return y

if __name__ == '__main__':
    pass