#!/usr/bin/python
#
# Get various Slurm part_metrics and feed them into an InfluxDB time-series database
# Xand Meaden, King's College London

import datetime
import grp
import influxdb
import json
import pyslurm
import pwd
import re
import socket
import struct
import sys
import time
import yaml

try:
    with open('config.yaml') as fh:
        config = yaml.load(fh)
except:
    sys.stderr.write('Failed to load configuration\n')
    sys.exit(1)

try:
    client = influxdb.InfluxDBClient(host=config['influxdb_host'], port=config['influxdb_port'], username=config['influxdb_username'], password=config['influxdb_password'], ssl=config['influxdb_ssl'])
    client.switch_database(config['influxdb_database'])
except:
    sys.stderr.write('Failed to connect to InfluxDB\n')
    sys.exit(2)

try:
    pyslurmnode = pyslurm.node()
except:
    sys.stderr.write('Failed to get Slurm data\n')
    sys.exit(3)

groups = config['groups']
partitions = pyslurm.partition().get()

node_partitions = {}

part_metrics = {}
part_metrics['cpu_total'] = {}
part_metrics['cpu_usage'] = {}
part_metrics['gpu_total'] = {}
part_metrics['gpu_usage'] = {}
part_metrics['mem_total'] = {}
part_metrics['mem_usage'] = {}
part_metrics['jobs_running'] = {}
part_metrics['jobs_pending'] = {}
part_metrics['queue_time'] = {}
part_metrics['queue_jobs'] = {}

user_metrics = {}
user_metrics['cpu_usage'] = {}
user_metrics['gpu_usage'] = {}
user_metrics['mem_usage'] = {}
user_metrics['jobs_running'] = {}
user_metrics['jobs_pending'] = {}
user_metrics['queue_time'] = {}
user_metrics['queue_jobs'] = {}

group_metrics = {}
group_metrics['cpu_usage'] = {}
group_metrics['gpu_usage'] = {}
group_metrics['mem_usage'] = {}
group_metrics['jobs_running'] = {}
group_metrics['jobs_pending'] = {}
group_metrics['queue_time'] = {}
group_metrics['queue_jobs'] = {}

user_ids = {}
user_groups = {}

now = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

# Setup data structures, with stats set to 0
for part in partitions.keys() + ['ALL']:
    if part != 'ALL':
        for node in partitions[part]['nodes'].split(','):
            if node not in node_partitions:
                node_partitions[node] = []
            node_partitions[node].append(part)

    part_metrics['cpu_total'][part] = 0
    part_metrics['cpu_usage'][part] = 0
    part_metrics['gpu_total'][part] = 0
    part_metrics['gpu_usage'][part] = 0
    part_metrics['mem_total'][part] = 0
    part_metrics['mem_usage'][part] = 0
    part_metrics['jobs_running'][part] = 0
    part_metrics['jobs_pending'][part] = 0
    part_metrics['queue_time'][part] = 0
    part_metrics['queue_jobs'][part] = 0

for group in groups:
    group_metrics['cpu_usage'][group] = 0
    group_metrics['gpu_usage'][group] = 0
    group_metrics['mem_usage'][group] = 0
    group_metrics['jobs_running'][group] = 0
    group_metrics['jobs_pending'][group] = 0
    group_metrics['queue_time'][group] = 0
    group_metrics['queue_jobs'][group] = 0

    members = grp.getgrnam(group)[3]
    for user in members:
        if user not in user_groups:
            user_groups[user] = []
        user_groups[user].append(group)

# Go through all the nodes and get their cpu/gpu/memory usage and store for each partition they belong to
nodes = pyslurmnode.get()
for node in nodes:
    node_data = nodes.get(node)

    part_metrics['cpu_total']['ALL'] += node_data['cpus']
    part_metrics['cpu_usage']['ALL'] += node_data['alloc_cpus']

    part_metrics['mem_total']['ALL'] += node_data['real_memory']
    part_metrics['mem_usage']['ALL'] += node_data['alloc_mem']

    gpu_total = 0
    gpu_usage = 0
    if node_data['gres']:
        gres_total = pyslurm.node().parse_gres(node_data['gres'][0])
        gres_usage = pyslurm.node().parse_gres(node_data['gres_used'][0])
        for g in gres_total:
            is_gpu = re.match(r'^gpu:([0-9]+)\(', g)
            if is_gpu:
                gpu_total = int(is_gpu.group(1))

        if gpu_total > 0:
            for g in gres_usage:
                is_gpu = re.match(r'^gpu:[^:]+:([0-9]+)\(', g)
                if is_gpu:
                    gpu_usage = int(is_gpu.group(1))

    part_metrics['gpu_total']['ALL'] += gpu_total
    part_metrics['gpu_usage']['ALL'] += gpu_usage

    for part in node_partitions[node]:
        part_metrics['cpu_total'][part] += node_data['cpus']
        part_metrics['cpu_usage'][part] += node_data['alloc_cpus']

        part_metrics['mem_total'][part] += node_data['real_memory']
        part_metrics['mem_usage'][part] += node_data['alloc_mem']

        part_metrics['gpu_total'][part] += gpu_total
        part_metrics['gpu_usage'][part] += gpu_usage

# Now go through the jobs list to see user-specific stuff
jobs = pyslurm.job().get()
for job in jobs:
    job = jobs.get(job)

    if job['user_id'] not in user_ids:
        user = pwd.getpwuid(job['user_id'])[0]
        user_ids[job['user_id']] = user

    if user not in user_metrics:
        user_metrics['cpu_usage'][user] = 0
        user_metrics['gpu_usage'][user] = 0
        user_metrics['mem_usage'][user] = 0
        user_metrics['jobs_running'][user] = 0
        user_metrics['jobs_pending'][user] = 0
        user_metrics['queue_time'][user] = 0
        user_metrics['queue_jobs'][user] = 0

    if job['job_state'] == 'RUNNING':
        part_metrics['jobs_running']['ALL'] += 1
        part_metrics['jobs_running'][job['partition']] += 1

        # This seems the only way to get a job's memory allocation, I think...
        tres_alloc = re.match(r'^cpu=([0-9]+),mem=([0-9.]+)(M|G),', job['tres_alloc_str'])
        cpu = int(tres_alloc.group(1))
        mem = float(tres_alloc.group(2))
        if tres_alloc.group(3) == 'G':
            mem *= 1024
        mem = int(mem)

        gpu = 0
        if job['tres_per_node']:
            tres_per_node = re.match(r'gpu:([0-9]+)', job['tres_per_node'])
            if tres_per_node:
                gpu = int(tres_per_node.group(1)) * job['num_nodes']

        user_metrics['jobs_running'][user] += 1
        user_metrics['cpu_usage'][user] += cpu
        user_metrics['gpu_usage'][user] += gpu
        user_metrics['mem_usage'][user] += mem

        queue_time = job['start_time'] - job['submit_time']
        user_metrics['queue_jobs'][user] += 1
        user_metrics['queue_time'][user] = (user_metrics['queue_time'][user] + queue_time) / user_metrics['queue_jobs'][user]
        part_metrics['queue_jobs']['ALL'] += 1
        part_metrics['queue_time']['ALL'] = (part_metrics['queue_time']['ALL'] + queue_time) / part_metrics['queue_jobs']['ALL']
        part_metrics['queue_jobs'][job['partition']] += 1
        part_metrics['queue_time'][job['partition']] = (part_metrics['queue_time'][job['partition']] + queue_time) / part_metrics['queue_jobs'][job['partition']]

        if user in user_groups:
            for group in user_groups[user]:
                group_metrics['jobs_running'][group] += 1
                group_metrics['cpu_usage'][group] += cpu
                group_metrics['gpu_usage'][group] += gpu
                group_metrics['mem_usage'][group] += mem
                group_metrics['queue_jobs'][group] += 1
                group_metrics['queue_time'][group] = (group_metrics['queue_time'][group] + queue_time) / group_metrics['queue_jobs'][group]

    elif job['job_state'] == 'PENDING':
        part_metrics['jobs_pending']['ALL'] += 1
        part_metrics['jobs_pending'][job['partition']] += 1

        user_metrics['jobs_pending'][user] += 1

        if user in user_groups:
            for group in user_groups[user]:
                group_metrics['jobs_pending'][group] += 1

payload = [
    {'measurement': 'partition_cpu_total', 'time': now, 'fields': part_metrics['cpu_total']},
    {'measurement': 'partition_cpu_usage', 'time': now, 'fields': part_metrics['cpu_usage']},
    {'measurement': 'partition_gpu_total', 'time': now, 'fields': part_metrics['gpu_total']},
    {'measurement': 'partition_gpu_usage', 'time': now, 'fields': part_metrics['gpu_usage']},
    {'measurement': 'partition_mem_total', 'time': now, 'fields': part_metrics['mem_total']},
    {'measurement': 'partition_mem_usage', 'time': now, 'fields': part_metrics['mem_usage']},
    {'measurement': 'partition_jobs_running', 'time': now, 'fields': part_metrics['jobs_running']},
    {'measurement': 'partition_jobs_pending', 'time': now, 'fields': part_metrics['jobs_pending']},
    {'measurement': 'partition_queue_time', 'time': now, 'fields': part_metrics['queue_time']},
    {'measurement': 'group_cpu_usage', 'time': now, 'fields': group_metrics['cpu_usage']},
    {'measurement': 'group_gpu_usage', 'time': now, 'fields': group_metrics['gpu_usage']},
    {'measurement': 'group_mem_usage', 'time': now, 'fields': group_metrics['mem_usage']},
    {'measurement': 'group_jobs_running', 'time': now, 'fields': group_metrics['jobs_running']},
    {'measurement': 'group_jobs_pending', 'time': now, 'fields': group_metrics['jobs_pending']},
    {'measurement': 'group_queue_time', 'time': now, 'fields': group_metrics['queue_time']},
    {'measurement': 'user_cpu_usage', 'time': now, 'fields': user_metrics['cpu_usage']},
    {'measurement': 'user_gpu_usage', 'time': now, 'fields': user_metrics['gpu_usage']},
    {'measurement': 'user_mem_usage', 'time': now, 'fields': user_metrics['mem_usage']},
    {'measurement': 'user_jobs_running', 'time': now, 'fields': user_metrics['jobs_running']},
    {'measurement': 'user_jobs_pending', 'time': now, 'fields': user_metrics['jobs_pending']},
    {'measurement': 'user_queue_time', 'time': now, 'fields': user_metrics['queue_time']},
]

client.write_points(payload)
