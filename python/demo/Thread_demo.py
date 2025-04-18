import threading

# # 创建线程本地对象
# local_data = threading.local()
#
# def worker():
#     if not hasattr(local_data, 'value'):
#         local_data.value = 42
#         print(f"begin set Thread {threading.current_thread().name}: {local_data.value}", "\n")
#
# # 创建多个线程
# threads = []
# for i in range(3):
#     t = threading.Thread(target=worker, name=f"Thread-{i}")
#     threads.append(t)
#     t.start()
#
# # 等待所有线程完成
# for t in threads:
#     t.join()

if __name__ == '__main__':
    run_id = ""
    if run_id:
        print("is empty")