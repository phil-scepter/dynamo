import sys
import subprocess
import multiprocessing as mp


def run_node(node_id):
    command = ['RUST_BACKTRACE=1 cargo run --package dynamo --bin dynamo "127.0.0.1" "{}"'.format(8000 + node_id)]
    subprocess.Popen(command, shell=True)


def main(nodes_count):
    for i in range(nodes_count):
        processes = [mp.Process(target=run_node, args=(node_id,)) for node_id in range(nodes_count)]

    for p in processes:
        p.start()

    for p in processes:
        p.join()


if __name__ == "__main__":
    nodes_count = sys.argv[1]

    main(int(nodes_count))
