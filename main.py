import numpy as np
import random

expired = []


class Task:
    def __init__(self, ID, task_type, birth_time, deadline):
        self.ID = ID
        self.task_type = task_type
        self.birth_time = birth_time
        self.deadline = deadline
        self.status = 0


class Core:
    def __init__(self, ID, alpha):
        self.ID = ID
        self.status = 'idle'
        self.task = None
        self.perform_time = -1
        self.alpha = alpha
        self.server = None

    def do_task(self):
        print(f'doing task {self.task.ID} by core {self.ID}')
        self.perform_time -= 1
        if self.perform_time == 0:
            print(f'task {self.task.ID} is done')
            self.task.status = 3
            self.status = 'idle'
            self.task = None
            self.perform_time = -1
            self.server.manage()

    def set_server(self, server: Server):
        self.server = server


class Server:
    def __init__(self, ID, cores):
        self.ID = ID
        self.Cores = cores
        self.Queue = []

    def check_expired(self, time):
        for i in range(len(self.Queue)):
            if self.Queue[i].deadline >= time:
                expired.append(self.Queue.pop(i))
                print(f'Time {self.time}: task {self.Queue[i].ID} with deadline {self.Queue[i].deadline} expired')


    def manage(self):
        for core in self.Cores:
            if core.status == 'idle':
                if len(self.Queue) == 0:
                    print(f'server {self.ID} Queue empty...')
                    break
                found_type_1 = False
                for i in range(len(self.Queue)):
                    if self.Queue[i].tast_type == 1:
                        print(f'Task rank {self.Queue[i].task_type} out of server {self.ID} Queue')
                        task = self.Queue.pop(i)
                        core.task = task
                        found_type_1 = True
                        break
                if not found_type_1:
                    print(f'Task rank 2 out of server {self.ID} Queue')
                    task = self.Queue.pop(0)
                    core.task = task
                time = np.random.exponential(core.alpha)
                core.perform_time = time
                core.status = 'busy'
                core.do_task()
            else:
                core.do_task()


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
            ttype = random.choices(types, probs)[0]
            deadline = np.random.exponential(self.Alpha)
            task = Task(ttype, self.time, deadline + self.time)
            self.Queue.append(task)
        if self.total >= 1000000:
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
        chosen_server = random.choices(candid_servers)[0]
        print(f'task assigned to server {chosen_server.ID}')
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
                    print(f'Task rank {self.Queue[j].task_type} out of sched Queue')
                    task = self.Queue.pop(j)
                    found_type_1 = True
                    break
            if not found_type_1:
                print(f'Task rank 2 out of sched Queue')
                task = self.Queue.pop(0)
            self.assign_to_server(task)

    def check_expired(self):
        for i in range(len(self.Queue)):
            if self.Queue[i].deadline >= self.time:
                expired.append(self.Queue.pop(i))
                print(f'Time {self.time}: task {self.Queue[i].ID} with deadline {self.Queue[i].deadline} expired')

    def manage(self):
        while True:
            self.check_expired()
            if self.total < 1000000:
                self.generate_tasks()
            self.sched_queue()
            for server in self.Servers:
                server.check_expired(self.time)
                server.manage()
            stop = True
            if self.total >= 1000000:
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
        core = Core(i+j, alphas[j])
        cores.append(core)
    server = Server(i, cores)
    for core in server.Cores:
        core.set_server(server)

scheduler.manage()