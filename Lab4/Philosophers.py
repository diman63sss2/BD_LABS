import threading
import time
import random
from kazoo.client import KazooClient

class Philosopher(threading.Thread):
    def __init__(self, philosopher_id, address, root, left_fork, right_fork, seats, mutex, iterations):
        super(Philosopher, self).__init__()

        self.id = philosopher_id
        self.address = address
        self.root = root
        self.left_fork = left_fork
        self.right_fork = right_fork
        self.seats = seats
        self.mutex = mutex
        self.iterations = iterations

        self.current_iteration = None
        self.zk = KazooClient(hosts=address)
        self.path = f"{root}/{philosopher_id}"

    def run(self):
        self.zk.start()
        for i in range(self.iterations):
            self.current_iteration = i
            self.eat()
            self.think()

    def eat(self):
        print(f"Philosopher {self.id} is going to eat")
        while True:
            if len(self.zk.get_children(self.root)) <= self.seats:
                self.zk.create(self.path, b"", ephemeral=True)

                self.mutex.acquire()
                self.left_fork.acquire()
                print(f"Philosopher {self.id} picked up the left fork")
                self.right_fork.acquire()
                print(f"Philosopher {self.id} picked up the right fork, eating")
                self.mutex.release()

                time.sleep((random.randint(0, 4) + 1) * 2)

                self.right_fork.release()
                print(f"Philosopher {self.id} put back the right fork")
                self.left_fork.release()
                print(f"Philosopher {self.id} put back the left fork, finished eating")

                break

    def think(self):
        print(f"Philosopher {self.id} is thinking")
        time.sleep((random.randint(0, 4) + 1) * 2)
        self.zk.delete(self.path)
        print(f"Philosopher {self.id} finished thinking")
        if self.current_iteration == self.iterations - 1:
            self.zk.stop()
            self.zk.close()

if __name__ == "__main__":
    iterations = 2
    address = "localhost:2181"
    philosophers_count = 5
    seats = philosophers_count
    mutex = threading.Semaphore(philosophers_count - 1)
    forks = [threading.Semaphore(1) for _ in range(philosophers_count)]
    threads = []

    zk = KazooClient(hosts=address)
    zk.start()
    if not zk.exists("/philosophers"):
        zk.create("/philosophers")
    zk.stop()
    zk.close()

    for philosopher_id in range(philosophers_count):
        philosopher = Philosopher(philosopher_id,
                                  address,
                                  "/philosophers",
                                  forks[philosopher_id],
                                  forks[(philosopher_id + 1) % philosophers_count],
                                  seats,
                                  mutex,
                                  iterations)
        thread = threading.Thread(target=philosopher.run, daemon=False)
        threads.append(thread)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
