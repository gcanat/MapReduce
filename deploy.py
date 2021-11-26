import subprocess as sub
#from threading import Thread
from multiprocessing import Pool
import sys
import time

TIMEOUT = 60


def erprint(*args, **kwargs):
    """Small function to print to stderr"""
    print(*args, file=sys.stderr, **kwargs)


def send_remote_cmd(host, cmd, timeout):
    """Send a shell command to a remote host"""
    with sub.Popen(cmd, stdout=sub.PIPE, stderr=sub.PIPE, text=True, encoding='utf8') as proc:
        try:
            stdout, stderr = proc.communicate(timeout=timeout)
        except sub.TimeoutExpired:
            proc.kill()
            stdout = ""
            stderr = f"{cmd} timed out after {timeout}s on host {host}"
    return stdout.strip("\n"), stderr.strip("\n")


def test_host(host):
    """Test if a host is replying to ssh connection"""
    host_state = {'up': None, 'dn': None}
    remote_cmd = '\"hostname\"'
    cmd = ['ssh', host, remote_cmd]
    stdout, stderr = send_remote_cmd(host, cmd, TIMEOUT)
    if stderr == "": 
        host_state['up'] = host
    else:
        host_state['dn'] = host
    return host_state, stderr


def deploy_file(file, host, target_path):
    """Upload a file on a remote host to target_path (creating the path if needed)"""
    remote_cmd = f"mkdir -p {target_path}"
    cmd = ['ssh', host, f'{remote_cmd}']
    stdout, stderr = send_remote_cmd(host, cmd, TIMEOUT)
    if stderr == "":
        cmd = ['scp', file, f'{host}:{target_path}']
        stdout, stderr = send_remote_cmd(host, cmd, TIMEOUT)
        return stdout, stderr
    else:
        return stdout, stderr


if __name__ == "__main__":
    time0 = time.time()
    if sys.argv[1] == "--help":
        print("Usage: python3 deploy.py <file to deploy> <hosts file> <target path>")
        print("example: python3 deploy.py slave.py hosts.txt /tmp/canat-20")
        exit(0)
    file = sys.argv[1]
    hostsfile = sys.argv[2]
    target_path = sys.argv[3]
    print(f"[DEPLOY] Uploading {file} to all hosts...")
    # add end slash if needed
    if not target_path.endswith("/"):
        target_path = target_path + "/"
    # list that will contain available hosts
    hosts_up = []
    errors = []

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
            erprint(f"[DEPLOY] ssh error: {x[1]}")

    # start second loop to deploy file on hosts that are up
    N = len(hosts_up)
    with Pool(30) as p:
        pool_res = p.starmap(deploy_file, zip([file]*N, hosts_up, [target_path]*N))

    for res in pool_res:
        if res[1] != "":
            errors.append(res[1])
    if len(errors) != 0:
        erprint(f"[DEPLOY] Upload Error: {errors}")
    else:
        duration = time.time() - time0
        print(f"[DEPLOY] All done! Duration: {round(duration,3)} sec")
