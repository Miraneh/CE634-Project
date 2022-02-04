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
        self.status = 'waiting'


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
            self.task.status = 'done'
            self.task.done_time = time
            self.status = 'idle'
            dones.append(self.task)
            self.task = None
            self.perform_time = -1
            self.server.manage(time)

        self.perform_time -= 1

        if self.perform_time == 0:
            print(f'task {self.task.ID} is done')
            print(f'core {self.ID} is idle')
            self.task.status = 'done'
            self.task.done_time = time
            self.status = 'idle'
            dones.append(self.task)
            self.task = None
            self.perform_time = -1
            self.server.manage(time)

    def set_server(self, server):
        self.server = server


class Server:
    def __init__(self, ID, cores):
        self.ID = ID
        self.Cores = cores
        self.Queue = []
        self.Queue_lengths = []

    def check_expired(self, time):
        to_be_popped = []
        for i in range(len(self.Queue)):
            if self.Queue[i].deadline <= time:
                to_be_popped.append(self.Queue[i])
                self.Queue[i].out_server_q = time
                self.Queue[i].status = 'expired'
                print(f'Time {time}: task {self.Queue[i].ID} with deadline {self.Queue[i].deadline} expired is Server Q')
        for item in to_be_popped:
            expired.append(item)
            self.Queue.remove(item)

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
        self.Queue_length = []

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
                self.Queue[i].status = 'expired'
                print(f'Time {self.time}: task {self.Queue[i].ID} with deadline {self.Queue[i].deadline} expired in Sched Q')
                self.Queue[i].out_sched_q = self.time
        for item in to_be_popped:
            expired.append(item)
            self.Queue.remove(item)

    def manage(self):
        while True:
            print(f'### TIME {self.time} ###')
            t.sleep(2)

            if self.total < task_total:
                self.generate_tasks()
            self.check_expired()
            self.sched_queue()
            self.Queue_length.append(len(self.Queue))
            for server in self.Servers:
                server.check_expired(self.time)
                server.manage(self.time)
                server.Queue_lengths.append(len(server.Queue))
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
print("\n\n")
print("# Average time spent in system:\n By type of task:\n")

avg_1 = []
avg_2 = []
for task in expired:
    if task.task_type == 1:
        avg_1.append(task.deadline - task.birth_time + 1)
    else:
        avg_2.append(task.deadline - task.birth_time + 1)
for task in dones:
    if task.task_type == 1:
        avg_1.append(task.done_time - task.birth_time + 1)
    else:
        avg_2.append(task.done_time - task.birth_time + 1)


print(f'Type 1 tasks: {0 if len(avg_1) == 0 else sum(avg_1)/len(avg_1)}\nType 2 tasks: {sum(avg_2)/len(avg_2)}\n')
print(f'Overall: {(sum(avg_1)+sum(avg_2))/(len(avg_1) + len(avg_2))}')
print()
print("# Average time spent in system:\nBy type:\n")

avg_q_1 = []
avg_q_2 = []

for task in expired:
    if task.task_type == 1:
        avg_q_1.append(task.out_sched_q - task.in_sched_q + task.out_server_q - task.in_server_q)
    else:
        avg_q_2.append(task.out_sched_q - task.in_sched_q + task.out_server_q - task.in_server_q)
for task in dones:
    if task.task_type == 1:
        avg_q_1.append(task.out_sched_q - task.in_sched_q + task.out_server_q - task.in_server_q)
    else:
        avg_q_2.append(task.out_sched_q - task.in_sched_q + task.out_server_q - task.in_server_q)

print(f'Type 1 tasks: {0 if len(avg_q_1) == 0 else sum(avg_q_1)/len(avg_q_1)}\nType 2 tasks: {sum(avg_q_2)/len(avg_q_2)}\n')
print(f'Overall: {(sum(avg_q_1)+sum(avg_q_2))/(len(avg_q_1) + len(avg_q_2))}')

print()
print("# Percentage of expired tasks\nBy type:\n")

exp_1 = 0
exp_2 = 0
for task in expired:
    if task.task_type == 1:
        exp_1 += 1
    else:
        exp_2 += 1
done_1 = 0
done_2 = 0
for task in dones:
    if task.task_type == 1:
        done_1 += 1
    else:
        done_2 += 1

print(f'Type 1 tasks: {0 if exp_1 + done_1 == 0 else (exp_1/(exp_1 + done_1)*100)}%\nType 2 tasks: {(exp_2/(exp_2 + done_2)*100)}%\n')
print(f'Overall: {(len(expired)/task_total)*100}%')

print()
print("# Average Queue length:\n")
print(f'Scheduler: {sum(scheduler.Queue_length)/len(scheduler.Queue_length)}')
for server in scheduler.Servers:
    print(f'Server {server.ID}: {sum(server.Queue_lengths)/len(server.Queue_lengths)}')


