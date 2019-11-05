#!/usr/bin/python
#
# Get various Slurm metrics['partition'] and feed them into an InfluxDB time-series database
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
    client = influxdb.InfluxDBClient(host=config['influxdb_host'], port=config['influxdb_port'], username=config['influxdb_username'], password=config['influxdb_password'], ssl=config['influxdb_ssl'], verify_ssl=config['influxdb_verify_ssl'])
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

metrics = {}
metrics['partition'] = {}
metrics['partition']['cpu_total'] = {}
metrics['partition']['cpu_usage'] = {}
metrics['partition']['cpu_usage_pc'] = {}
metrics['partition']['gpu_total'] = {}
metrics['partition']['gpu_usage'] = {}
metrics['partition']['gpu_usage_pc'] = {}
metrics['partition']['mem_total'] = {}
metrics['partition']['mem_usage'] = {}
metrics['partition']['mem_usage_pc'] = {}
metrics['partition']['jobs_running'] = {}
metrics['partition']['jobs_pending'] = {}
metrics['partition']['queue_time'] = {}
metrics['partition']['queue_jobs'] = {}

metrics['user'] = {}
metrics['user']['cpu_usage'] = {}
metrics['user']['gpu_usage'] = {}
metrics['user']['mem_usage'] = {}
metrics['user']['jobs_running'] = {}
metrics['user']['jobs_pending'] = {}
metrics['user']['queue_time'] = {}
metrics['user']['queue_jobs'] = {}

metrics['group'] = {}
metrics['group']['cpu_usage'] = {}
metrics['group']['gpu_usage'] = {}
metrics['group']['mem_usage'] = {}
metrics['group']['jobs_running'] = {}
metrics['group']['jobs_pending'] = {}
metrics['group']['queue_time'] = {}
metrics['group']['queue_jobs'] = {}

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

    metrics['partition']['cpu_total'][part] = 0
    metrics['partition']['cpu_usage'][part] = 0
    metrics['partition']['cpu_usage_pc'][part] = 0
    metrics['partition']['gpu_total'][part] = 0
    metrics['partition']['gpu_usage'][part] = 0
    metrics['partition']['gpu_usage_pc'][part] = 0
    metrics['partition']['mem_total'][part] = 0
    metrics['partition']['mem_usage'][part] = 0
    metrics['partition']['mem_usage_pc'][part] = 0
    metrics['partition']['jobs_running'][part] = 0
    metrics['partition']['jobs_pending'][part] = 0
    metrics['partition']['queue_time'][part] = 0
    metrics['partition']['queue_jobs'][part] = 0

for group in groups:
    metrics['group']['cpu_usage'][group] = 0
    metrics['group']['gpu_usage'][group] = 0
    metrics['group']['mem_usage'][group] = 0
    metrics['group']['jobs_running'][group] = 0
    metrics['group']['jobs_pending'][group] = 0
    metrics['group']['queue_time'][group] = 0
    metrics['group']['queue_jobs'][group] = 0

    members = grp.getgrnam(group)[3]
    for user in members:
        if user not in user_groups:
            user_groups[user] = []
        user_groups[user].append(group)

# Go through all the nodes and get their cpu/gpu/memory usage and store for each partition they belong to
nodes = pyslurmnode.get()
for node in nodes:
    node_data = nodes.get(node)

    metrics['partition']['cpu_total']['ALL'] += node_data['cpus']
    metrics['partition']['cpu_usage']['ALL'] += node_data['alloc_cpus']
    metrics['partition']['cpu_usage_pc']['ALL'] = 100 * (float(metrics['partition']['cpu_usage']['ALL']) / float(metrics['partition']['cpu_total']['ALL']))

    metrics['partition']['mem_total']['ALL'] += node_data['real_memory'] * 1048576
    metrics['partition']['mem_usage']['ALL'] += node_data['alloc_mem'] * 1048576
    metrics['partition']['mem_usage_pc']['ALL'] = 100 * (float(metrics['partition']['mem_usage']['ALL']) / float(metrics['partition']['mem_total']['ALL']))

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

    metrics['partition']['gpu_total']['ALL'] += gpu_total
    metrics['partition']['gpu_usage']['ALL'] += gpu_usage
    if metrics['partition']['gpu_total']['ALL'] > 0:
        metrics['partition']['gpu_usage_pc']['ALL'] = 100 * (float(metrics['partition']['gpu_usage']['ALL']) / metrics['partition']['gpu_total']['ALL'])

    for part in node_partitions[node]:
        metrics['partition']['cpu_total'][part] += node_data['cpus']
        metrics['partition']['cpu_usage'][part] += node_data['alloc_cpus']
        metrics['partition']['cpu_usage_pc'][part] = 100 * (float(metrics['partition']['cpu_usage'][part]) / metrics['partition']['cpu_total'][part])

        metrics['partition']['mem_total'][part] += node_data['real_memory'] * 1048576
        metrics['partition']['mem_usage'][part] += node_data['alloc_mem'] * 1048576
        metrics['partition']['mem_usage_pc'][part] = 100 * (float(metrics['partition']['mem_usage'][part]) / metrics['partition']['mem_total'][part])

        metrics['partition']['gpu_total'][part] += gpu_total
        metrics['partition']['gpu_usage'][part] += gpu_usage
        if metrics['partition']['gpu_total'][part] > 0:
            metrics['partition']['gpu_usage_pc'][part] = 100 * (float(metrics['partition']['gpu_usage'][part]) / metrics['partition']['gpu_total'][part])

# Now go through the jobs list to see user-specific stuff
jobs = pyslurm.job().get()
for job in jobs:
    job = jobs.get(job)

    if job['user_id'] not in user_ids:
        user = pwd.getpwuid(job['user_id'])[0]
        user_ids[job['user_id']] = user
        metrics['user']['cpu_usage'][user] = 0
        metrics['user']['gpu_usage'][user] = 0
        metrics['user']['mem_usage'][user] = 0
        metrics['user']['jobs_running'][user] = 0
        metrics['user']['jobs_pending'][user] = 0
        metrics['user']['queue_time'][user] = 0
        metrics['user']['queue_jobs'][user] = 0

    if job['job_state'] == 'RUNNING':
        metrics['partition']['jobs_running']['ALL'] += 1
        metrics['partition']['jobs_running'][job['partition']] += 1

        # This seems the only way to get a job's memory allocation, I think...
        tres_alloc = re.match(r'^cpu=([0-9]+),mem=([0-9.]+)(M|G),', job['tres_alloc_str'])
        cpu = int(tres_alloc.group(1))
        mem = float(tres_alloc.group(2))
        if tres_alloc.group(3) == 'G':
            mem *= 1024
        mem *= 1048576
        mem = int(mem)

        gpu = 0
        if job['tres_per_node']:
            tres_per_node = re.match(r'gpu:([0-9]+)', job['tres_per_node'])
            if tres_per_node:
                gpu = int(tres_per_node.group(1)) * job['num_nodes']

        metrics['user']['jobs_running'][user] += 1
        metrics['user']['cpu_usage'][user] += cpu
        metrics['user']['gpu_usage'][user] += gpu
        metrics['user']['mem_usage'][user] += mem

        queue_time = job['start_time'] - job['submit_time']
        metrics['user']['queue_jobs'][user] += 1
        metrics['user']['queue_time'][user] = (float(metrics['user']['queue_time'][user] + queue_time)) / metrics['user']['queue_jobs'][user]
        metrics['partition']['queue_jobs']['ALL'] += 1
        metrics['partition']['queue_time']['ALL'] = (float(metrics['partition']['queue_time']['ALL'] + queue_time)) / metrics['partition']['queue_jobs']['ALL']
        metrics['partition']['queue_jobs'][job['partition']] += 1
        metrics['partition']['queue_time'][job['partition']] = (float(metrics['partition']['queue_time'][job['partition']] + queue_time)) / metrics['partition']['queue_jobs'][job['partition']]

        if user in user_groups:
            for group in user_groups[user]:
                metrics['group']['jobs_running'][group] += 1
                metrics['group']['cpu_usage'][group] += cpu
                metrics['group']['gpu_usage'][group] += gpu
                metrics['group']['mem_usage'][group] += mem
                metrics['group']['queue_jobs'][group] += 1
                metrics['group']['queue_time'][group] = (float(metrics['group']['queue_time'][group] + queue_time)) / metrics['group']['queue_jobs'][group]

    elif job['job_state'] == 'PENDING':
        metrics['partition']['jobs_pending']['ALL'] += 1
        metrics['partition']['jobs_pending'][job['partition']] += 1

        metrics['user']['jobs_pending'][user] += 1

        if user in user_groups:
            for group in user_groups[user]:
                metrics['group']['jobs_pending'][group] += 1

payload = []
for grouping in ['partition', 'user', 'group']:
    for reading in ['cpu_total', 'cpu_usage', 'cpu_usage_pc', 'gpu_total', 'gpu_usage', 'gpu_usage_pc', 'mem_total', 'mem_usage', 'mem_usage_pc', 'jobs_running', 'jobs_pending', 'queue_time']:
        if reading in metrics[grouping] and len(metrics[grouping][reading]) > 0:
            for key in metrics[grouping][reading].keys():
                payload.append({'measurement': '%s_%s' % (grouping, reading), 'time': now, 'fields': {reading: metrics[grouping][reading][key]}, 'tags': {grouping: key}})

client.write_points(payload)
