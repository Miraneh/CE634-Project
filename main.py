import numpy as np
import random
import math
import time as t

task_total = 15
expired = []
dones = []


class Task:
    def __init__(self, ID, task_type, birth_time, deadline):
        self.ID = ID
        self.task_type = task_type
        self.birth_time = birth_time
        self.deadline = deadline
        self.done_time = -1
        self.in_sched_q = 0
        self.out_sched_q = 0
        self.in_server_q = 0
        self.out_server_q = 0
        self.status = 0


class Core:
    def __init__(self, ID, alpha):
        self.ID = ID
        self.status = 'idle'
        self.task = None
        self.perform_time = -1
        self.alpha = alpha
        self.server = None

    def do_task(self, time):
        print(f'doing task {self.task.ID} by core {self.ID}')
        print(f'Perform time: {self.perform_time}')
        if self.perform_time == 0:
            print(f'task {self.task.ID} is done')
            print(f'core {self.ID} is idle')
            self.task.status = 3
            self.task.done_time = time
            self.status = 'idle'
            dones.append(self.task)
            self.task = None
            self.perform_time = -1
            self.server.manage()

        self.perform_time -= 1

        if self.perform_time == 0:
            print(f'task {self.task.ID} is done')
            print(f'core {self.ID} is idle')
            self.task.status = 3
            self.task.done_time = time
            self.status = 'idle'
            dones.append(self.task)
            self.task = None
            self.perform_time = -1
            self.server.manage()

    def set_server(self, server):
        self.server = server


class Server:
    def __init__(self, ID, cores):
        self.ID = ID
        self.Cores = cores
        self.Queue = []

    def check_expired(self, time):
        to_be_popped = []
        for i in range(len(self.Queue)):
            if self.Queue[i].deadline <= time:
                to_be_popped.append(self.Queue[i])
                self.Queue[i].out_server_q = time
                print(f'Time {time}: task {self.Queue[i].ID} with deadline {self.Queue[i].deadline} expired is Server Q')
        for item in to_be_popped:
            expired.append(self.Queue.remove(item))

    def manage(self, real_time):
        for core in self.Cores:
            if core.status == 'idle':
                if len(self.Queue) == 0:
                    print(f'server {self.ID} Queue empty...')
                    continue
                found_type_1 = False
                for i in range(len(self.Queue)):
                    if self.Queue[i].task_type == 1:
                        print(f'Task {self.Queue[i].ID} rank {self.Queue[i].task_type} out of server {self.ID} Queue')
                        task = self.Queue.pop(i)
                        core.task = task
                        found_type_1 = True
                        break
                if not found_type_1:
                    print(f'Task {self.Queue[0].ID} rank 2 out of server {self.ID} Queue')
                    task = self.Queue.pop(0)
                    core.task = task
                core.task.out_server_q = real_time
                time = math.floor(np.random.exponential(core.alpha))
                core.perform_time = time
                core.status = 'busy'
            core.do_task(real_time)


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
        print(f'Generating {num_tasks_generated} tasks')
        if num_tasks_generated + self.total > task_total:
            num_tasks_generated = task_total - self.total
        else:
            self.total += num_tasks_generated
        for i in range(num_tasks_generated):
            types = [1, 2]
            probs = [.1, .9]
            ttype = random.choices(types, probs)[0]
            deadline = math.floor(np.random.exponential(self.Alpha))
            task = Task(self.time + i, ttype, self.time, deadline + self.time)
            print(f'task {task.ID}, type {ttype}, deadline {task.deadline}')
            task.in_sched_q = self.time
            self.Queue.append(task)
        if self.total >= task_total:
            print(f'reached total at time {self.time}, enough generation...')
            return True
        else:
            return False

    def assign_to_server(self, task: Task):
        mini = 2000000
        candid_servers = []
        for server in self.Servers:
            if len(server.Queue) < mini:
                candid_servers = [server]
                mini = len(server.Queue)
            elif len(server.Queue) == mini:
                candid_servers.append(server)
        chosen_server = random.choice(candid_servers)
        print(f'task {task.ID} assigned to server {chosen_server.ID}')
        chosen_server.Queue.append(task)

    def sched_queue(self):
        service_rate = np.random.poisson(self.Mio)
        for i in range(service_rate):
            found_type_1 = False
            if len(self.Queue) == 0:
                print(f'sched Queue empty...')
                break
            for j in range(len(self.Queue)):
                if self.Queue[j].task_type == 1:
                    print(f'Task {self.Queue[j].ID} rank {self.Queue[j].task_type} out of sched Queue')
                    task = self.Queue.pop(j)
                    found_type_1 = True
                    break
            if not found_type_1:
                print(f'Task {self.Queue[0].ID} rank 2 out of sched Queue')
                task = self.Queue.pop(0)

            task.out_sched_q = self.time
            task.in_server_q = self.time
            self.assign_to_server(task)

    def check_expired(self):
        to_be_popped = []
        for i in range(len(self.Queue)):
            if self.Queue[i].deadline <= self.time:
                to_be_popped.append(self.Queue[i])
                print(f'Time {self.time}: task {self.Queue[i].ID} with deadline {self.Queue[i].deadline} expired in Sched Q')
                self.Queue[i].out_sched_q = self.time
        for item in to_be_popped:
            expired.append(self.Queue.remove(item))

    def manage(self):
        while True:
            print(f'### TIME {self.time} ###')
            t.sleep(3)

            if self.total < task_total:
                self.generate_tasks()
            self.check_expired()
            self.sched_queue()
            for server in self.Servers:
                server.check_expired(self.time)
                server.manage(self.time)
            stop = False
            if self.total >= task_total and len(self.Queue) == 0:
                stop = True
                for server in self.Servers:
                    if len(server.Queue) == 0:
                        for core in server.Cores:
                            if core.status == 'busy':
                                stop = False
                                break
                        if not stop:
                            break
            if stop:
                print(f'### Simulation Done ###')
                break
            self.time += 1


landa, alpha, mio = input().split()
scheduler = Scheduler(int(landa), int(alpha), int(mio))
for i in range(5):
    alphas = input().split()
    cores = []
    for j in range(3):
        core = Core(i+j, int(alphas[j]))
        cores.append(core)
    server = Server(i, cores)
    for core in server.Cores:
        core.set_server(server)
    scheduler.Servers.append(server)

scheduler.manage()
print("\n\n")
print("### STATS ###")
print(f'{len(expired)} tasks expired')
print(f'{len(dones)} tasks done')
