import subprocess as sub
#from threading import Thread
from multiprocessing import Pool
import sys
import time

TIMEOUT = 200

threads1 = []
threads2 = []
results = []

def erprint(*args, **kwargs):
    """Small function to print to stderr"""
    print(*args, file=sys.stderr, **kwargs)


def send_remote_cmd(host, cmd, timeout):
    with sub.Popen(cmd, stdout=sub.PIPE, stderr=sub.PIPE, text=True, encoding='utf8') as proc:
        try:
            stdout, stderr = proc.communicate(timeout=timeout)
        except sub.TimeoutExpired:
            proc.kill()
            stdout = ""
            stderr = f"{cmd} timed out after {timeout}s on host {host}"
    return stdout.strip("\n"), stderr.strip("\n")


def test_host(host):
    host_state = {'up':None, 'dn': None}
    remote_cmd = '\"hostname\"'
    cmd = ['ssh', host, remote_cmd]
    stdout, stderr = send_remote_cmd(host, cmd, TIMEOUT)
    if stderr == "": 
        host_state['up'] = host
    else:
        host_state['dn'] = host
    return host_state, stderr

def clean_host(host, target_path):
    remote_cmd = f"rm -rf {target_path}"
    cmd = ['ssh', host, f'{remote_cmd}']
    stdout, stderr = send_remote_cmd(host, cmd, TIMEOUT)
    return stdout, stderr


if __name__ == "__main__":
    time0 = time.time()
    if sys.argv[1] == "--help":
        print("Usage: python3 clean.py <hosts file> <target path>")
        print("Example: python3 clean.py hosts.txt /tmp/canat-20/")
        exit(0)
    hostsfile = sys.argv[1]
    target_path = sys.argv[2]
    # list that will contain available hosts
    hosts_up = []
    errors = []

    print("[CLEANUP] Starting cleanup on all hosts")
    with open(hostsfile) as f:
        # get list of hosts
        hosts_list = f.read().split("\n")[:-1]  # we dont take last element because it's empty
    # first loop to get the list of hosts that are up
    with Pool(30) as p:
        pool_res = p.map(test_host, hosts_list)
    for i, x in enumerate(pool_res):
        if x[0]['up'] is not None:
            hosts_up.append(x[0]['up'])
        else:
            errors.append(x[1])

    print(hosts_up)
    # start second loop to clean the dir
    with Pool(30) as p:
        pool_res = p.starmap(clean_host, zip(hosts_up, [target_path]*len(hosts_up)))

    for res in pool_res:
        if res[1] != "":
            errors.append(res[1])

    duration = time.time() - time0
    num_errors = 0
    for i, err in enumerate(errors):
        if err != "":
            erprint(f"Error encountered: {err}")
            num_errors += 1
    if num_errors == 0:
        print("[CLEANUP] All done without errors!")
    else:
        erprint(f"{num_errors} error(s) encountered")
    print(f"[CLEANUP] Duration: {round(duration,3)} sec")
