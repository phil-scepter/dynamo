import sys
import subprocess
import multiprocessing as mp
from threading import Lock

pids = []
pid_lock = Lock()

def run_node(start_port, node_id):
    command = ['RUST_BACKTRACE=1 cargo run --package dynamo --bin dynamo "127.0.0.1" "{}"'.format(start_port + node_id)]
    pipe = subprocess.Popen(command, shell=True)
    print("pid is {}".format(pipe.pid))

    pid_lock.acquire()
    pids += pid
    pid_lock.release()


def kill_process(pid):
    command = ['kill {}'.format(pid)]
    subprocess.Popen(command, shell=True)


def main(start_port, nodes_count):
    for i in range(nodes_count):
        processes = [mp.Process(target=run_node, args=(start_port, node_id,)) for node_id in range(nodes_count)]

    for p in processes:
        p.start()

    for p in processes:
        p.join()

    while True:
        try:
           discard = input("Enter Ctrl-C or Ctrl-D to exit")
        except SyntaxError:
            pass
        except (KeyboardInterrupt, EOFError):
            pid_lock.acquire()
            for pid in pids:
                kill_process(pid)
            pid_lock.release()
            sys.exit(1)


if __name__ == "__main__":
    start_port = sys.argv[1]
    nodes_count = sys.argv[2]

    main(int(start_port), int(nodes_count))
