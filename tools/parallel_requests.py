# This script creates parallel processes to send predict requests

import argparse
import json
import requests
import time
from multiprocessing import Process, Queue, Pipe

_predict_endpoint = "/predict/"
_health_endpoint = "/health/"
_stats_endpoint = "/statsinternal/"


class SenderStats:
    def __init__(self, name):
        self.name = name
        self.requests_to_send = None
        self.responses_received = 0
        self.requests_failed = 0


class WorkerStats:
    def __init__(self):
        self.workers = {}


def request_func(queue, server_url, num_request, dataset_path, name):
    stats = SenderStats(name)
    stats.requests_to_send = num_request
    for _ in range(num_request):
        with open(dataset_path) as f:
            response = requests.post(server_url + _predict_endpoint, files={"X": f})
            if response.ok:
                stats.responses_received += 1
            else:
                stats.requests_failed += 1
    queue.put(stats)


def stats_func(pipe, server_url):
    worker_stats = WorkerStats()
    while True:
        for _ in range(10):
            time.sleep(0.1)
            response = requests.post(server_url + _stats_endpoint)
            if response.ok:
                dd = json.loads(response.text)
                worker_id = dd["sys_info"]["wuuid"]
                predict_calls_count = dd["predict_calls_per_worker"]

                worker_stats.workers[str(worker_id)] = predict_calls_count
                worker_stats.workers["total"] = dd["predict_calls_total"]

        o = ""
        if pipe.poll():
            o = pipe.recv()
        if o == "shutdown":
            pipe.send(worker_stats)
            break

        time.sleep(2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parallel requests sender")
    parser.add_argument("--input", required=True, help="Input dataset")
    parser.add_argument("--requests", default=1, type=int, help="Number of requests")
    parser.add_argument("--threads", default=1, type=int, help="Number of clients")
    parser.add_argument(
        "--address", default=None, required=True, help="Prediction server address host:port"
    )

    args = parser.parse_args()

    host_port_list = args.address.split(":", 1)
    host = host_port_list[0]
    port = int(host_port_list[1]) if len(host_port_list) == 2 else None
    url_server_address = "http://{}:{}".format(host, port)

    remainder = args.requests % args.threads
    if remainder:
        requests_per_thread = int(args.requests / args.threads) + 1
    else:
        requests_per_thread = int(args.requests / args.threads)

    processes = []
    q = Queue()
    main_conn, worker_stats_conn = Pipe()

    stats_thread = Process(target=stats_func, args=(worker_stats_conn, url_server_address,))
    stats_thread.start()

    for i in range(args.threads):
        p = Process(
            target=request_func, args=(q, url_server_address, requests_per_thread, args.input, i,)
        )
        processes.append(p)
    for p in processes:
        p.start()
        p.join()

    main_conn.send("shutdown")
    stats_thread.join()
    workers_stats = main_conn.recv()

    total_requests = 0
    total_responses = 0
    total_failed = 0
    for i in range(args.threads):
        stats = q.get()
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        print("Stats from sender: {}".format(stats.name))
        print("    Requests to send: {}".format(stats.requests_to_send))
        print("    Requests succeeded: {}".format(stats.responses_received))
        print("    Requests failed: {}".format(stats.requests_failed))
        print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")
        total_requests += stats.requests_to_send
        total_responses += stats.responses_received
        total_failed += stats.requests_failed

    print("Summary:")
    print("    Total to send: {}".format(total_requests))
    print("    Total succeeded: {}".format(total_responses))
    print("    Total failed: {}".format(total_failed))

    print("\n\nWorkers stats:")
    total_predicted_on_workers = 0
    for key, value in workers_stats.workers.items():
        if key != "total":
            print("   worker: {}; predicsts: {}".format(key, value))
            total_predicted_on_workers += value
    print("\n")
    print("Total predicted on workers: {}".format(total_predicted_on_workers))
    print(
        "Total predicted on workers (metrics by uwsgi): {}".format(workers_stats.workers["total"])
    )
