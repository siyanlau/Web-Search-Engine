import threading
import time
import random

# Shared list (our "frontier" stand-in)
frontier = []
frontier_lock = threading.Lock()

def worker(thread_id):
    while True:
        # Acquire lock to safely read/write shared list
        with frontier_lock:
            if not frontier:   # frontier empty → exit
                print(f"Thread-{thread_id} exiting: frontier empty")
                return
            item = frontier.pop(0)   # take first item
        # Work outside lock
        print(f"Thread-{thread_id} processing {item}")
        time.sleep(random.uniform(0.2, 1.0))  # simulate work

def main():
    global frontier
    frontier = [f"task-{i}" for i in range(10)]  # preload 10 tasks

    threads = []
    for i in range(3):   # 3 worker threads
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()  # wait for all to finish

    print("All threads finished")

if __name__ == "__main__":
    main()
