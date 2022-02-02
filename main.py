import numpy as np
import random


class Task:
    def __init__(self, task_type, birth_time, deadline):
        self.task_type = task_type
        self.birth_time = birth_time
        self.deadline = deadline
        self.situation = 0


class Core:
    def __init__(self):
        self.situation = 'idle'



class Server:
    def __init__(self, cores):
        self.Cores = cores
        self.Queue = []


class Scheduler:
    def __init__(self, Lambda, Alpha, Mio):
        self.Queue = []
        self.Servers = []
        self.time = 0
        self.Lambda = Lambda
        self.Alpha = Alpha
        self.total = 0
        self.Mio = Mio

    def generate_tasks(self):
        num_tasks_generated = np.random.poisson(self.Lambda)
        if num_tasks_generated + self.total > 1000000:
            num_tasks_generated = 1000000 - self.total
        else:
            self.total += num_tasks_generated
        for i in range(num_tasks_generated):
            types = [1, 2]
            probs = [.1, .9]
            ttype = random.choices(types, probs)
            deadline = np.random.exponential(self.Alpha)
            task = Task(ttype, self.time, deadline + self.time)
            self.Queue.append(task)
        if self.total >= 1000000:
            print(f'reached total at time {self.time}, enough generation...')
            return True
        else:
            return False

    def assign_to_server(self, task:Task):
        mini = 2000000
        candid_servers = []
        for server in self.Servers:
            if len(server.Queue) < mini:
                candid_servers = [server]
                mini = len(server.Queue)
            elif len(server.Queue) == mini:
                candid_servers.append(server)
        chosen_server = random.choices()


    def sched_queue(self):
        service_rate = np.random.poisson(self.Mio)
        for i in range(service_rate):
            found_type_1 = False
            if len(self.Queue) == 0:
                print(f'sched Queue empty...')
                break
            for j in range(len(self.Queue)):
                if self.Queue[j].task_type == 1:
                    print(f'Task rank {self.Queue[j].task_type} out of sched Queue')
                    task = self.Queue.pop(j)
                    found_type_1 = True
                    break
            if not found_type_1:
                print(f'Task rank 2 out of sched Queue')
                task = self.Queue.pop(0)

    def manage(self):
        while True:
            if self.total < 1000000:
                self.generate_tasks()
            self.sched_queue()


