import subprocess as sub
import sys
import time
import random
import re
import os
from multiprocessing import Pool
import zipfile
import argparse

DEBUG = False
BASE_PATH = "/tmp/canat-20"
SSH_TIMEOUT = 10
SCRIPT_TIMEOUT = 600
ZIP_REDUCE = False
# MB_SIZE = 24
# HINT_SIZE = MB_SIZE * 1024 * 1000
# MAX_REDUCE_HOSTS = 20
results = []


def erprint(*args, **kwargs):
    """Small function to print to stderr"""
    print(*args, file=sys.stderr, **kwargs)


def make_splits(
    input_file,
    target_path,
    max_num_splits=None,
    size_hint=30_000_000,
    hosts_list=None,
    split_path=None,
    master_host=None,
):
    """
    Split function with two methods:
    - split into a fixed number of splits (using max_num_splits)
    - otherwise split into a fixed sized blocks as defined (approximately) by
      size_hint
    """
    # create master split path
    cmd = ["mkdir", "-p", target_path]
    exec_cmd(cmd, SSH_TIMEOUT)

    results = []
    selected_hosts = []
    # create empty dict to store which splits go to which host
    splits = dict(zip(hosts_list, [""] * len(hosts_list)))

    with Pool(30) as pool:
        # fixed number of splits
        if max_num_splits is not None:
            with open(
                input_file, "rt", errors="ignore", buffering=100 * (2 ** 20)
            ) as f:
                lines = f.readlines()
            num_lines = len(lines)
            if num_lines < max_num_splits:
                num_splits = num_lines
            else:
                num_splits = max_num_splits
            split_size = num_lines // num_splits
            k = 0
            i = 0
            while i < num_splits:
                filename = "S" + str(i) + ".txt"
                outfile = target_path + filename
                with open(outfile, "w", 100 * (2 ** 20)) as sf:
                    split_start = min(k, num_lines)
                    split_end = min(k + split_size, num_lines)
                    sf.write("\n".join(lines[split_start:split_end]))
                # select host and upload file
                selec_host = random.choice(hosts_list)
                selected_hosts.append(selec_host)
                hosts_list.remove(selec_host)
                args = (target_path + filename, selec_host, split_path, master_host)
                results.append(pool.apply_async(deploy_file, args))

                k += split_size
                i += 1
            all_res = [res.get() for res in results]
            return num_splits, selected_hosts

        # fixed block size splits
        else:
            file_number = 0
            with open(
                input_file, "rt", errors="ignore", buffering=100 * (2 ** 20)
            ) as f:
                while True:
                    buf = f.readlines(size_hint)
                    if not buf:
                        # we've read the entire file in, so we're done.
                        break
                    filename = "S" + str(file_number) + ".txt"
                    outfile_path = target_path + filename
                    with open(outfile_path, "wt", 100 * (2 ** 20)) as of:
                        of.write("\n".join(buf))
                    file_number += 1
                    # select host and upload file
                    selec_host = hosts_list[hash(filename) % len(hosts_list)]
                    selected_hosts.append(selec_host)
                    splits[selec_host] += f"{filename} "
                    args = (
                        target_path + filename,
                        selec_host,
                        split_path,
                        master_host,
                    )
                    results.append(pool.apply_async(deploy_file, args))
                all_res = [res.get() for res in results]
            return file_number, splits


def send_remote_cmd(host, cmd, timeout):
    """
    Utility function to send shell commands to remote host
    """
    with sub.Popen(
        cmd, stdout=sub.PIPE, stderr=sub.PIPE, text=True, encoding="utf8"
    ) as proc:
        try:
            stdout, stderr = proc.communicate(timeout=timeout)
        except sub.TimeoutExpired:
            proc.kill()
            stdout = ""
            stderr = f"{cmd} timed out after {timeout}s on host {host}"
    return stdout.strip("\n"), stderr.strip("\n")


def test_host(host, base_path):
    """Test if a host responds to ssh"""
    host_state = {"up": None, "dn": None}
    remote_cmd = f"mkdir -p {base_path}splits && mkdir -p {base_path}maps && mkdir -p {base_path}reduces && mkdir -p {base_path}shuffles && mkdir -p {base_path}shufflesreceived"
    cmd = ["ssh", host, f"{remote_cmd}"]
    stdout, stderr = send_remote_cmd(host, cmd, SSH_TIMEOUT)
    if stderr == "":
        host_state["up"] = host
    else:
        host_state["dn"] = host
    return host_state, stderr


def exec_cmd(cmd, timeout):
    """Execute a shell command on local host"""
    with sub.Popen(
        cmd, stdout=sub.PIPE, stderr=sub.PIPE, text=True, encoding="utf8"
    ) as proc:
        try:
            stdout, stderr = proc.communicate(timeout=timeout)
        except sub.TimeoutExpired:
            proc.kill()
            stdout = ""
            stderr = f"{cmd} timed out after {timeout}s"
    # return stdout.strip("\n"), stderr.strip("\n")
    if stderr != "":
        erprint(stderr)
    return stdout, stderr


def deploy_file(file, host, target_path, source_host):
    """Upload `file` on `host` into `target_path`"""
    if host == source_host:
        cmd = ["cp", "-r", file, target_path]
        stdout, stderr = exec_cmd(cmd, SSH_TIMEOUT)
    else:
        cmd = ["scp", file, f"canat-20@{host}:{target_path}"]
        stdout, stderr = send_remote_cmd(host, cmd, SSH_TIMEOUT)
        if stderr != "":
            wait_time = random.randint(1, 5)
            time.sleep(wait_time)
            stdout, stderr = send_remote_cmd(host, cmd, SSH_TIMEOUT)
    return stdout, stderr


def run_remote_script(host, path_to_file, script_args):
    """Run file on remote host"""
    remote_cmd = f"export PYTHONHASHSEED=0;python3 {path_to_file} {script_args}"
    cmd = ["ssh", host, f"{remote_cmd}"]
    stdout, stderr = send_remote_cmd(host, cmd, SCRIPT_TIMEOUT)
    return stdout, stderr


def unzip(file):
    """Unzip the whole content of a zip file"""
    with zipfile.ZipFile(file, mode="r") as myzip:
        myzip.extractall()
    # delete zip file once it is extracted
    os.remove(file)


def check_results(pool_res, args_list):
    results = dict(
        zip([x[0] for x in args_list], [dict(success=[], errors=[])] * len(args_list))
    )
    for i, (stdout, stderr) in enumerate(pool_res):
        if stderr == "":
            results[args_list[i][0]]["success"] += [stdout]
        else:
            results[args_list[i][0]]["errors"] += [stderr]
    errors = 0
    for host in results.keys():
        if len(results[host]["errors"]) > 0:
            errors += 1
            erprint(f"Error on {host}: {results[host]['errors']}")
    if errors == 0:
        return 1, results
    else:
        return 0, results


if __name__ == "__main__":
    # setting up arguments of the script
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", "-i", help="input file to process")
    parser.add_argument(
        "--script_path",
        "-sp",
        default="slave.py",
        help="path to worker script, eg slave.py",
    )
    parser.add_argument(
        "--target_path",
        "-tp",
        default=BASE_PATH,
        help="target base path, eg. /tmp/username",
    )
    parser.add_argument(
        "--hosts_file", "-hf", default="hosts.txt", help="hosts file, eg. hosts.txt"
    )
    parser.add_argument(
        "--split_size", "-sz", default=32, type=int, help="split size in MB"
    )
    parser.add_argument(
        "--map_hosts",
        "-mh",
        default=5,
        type=int,
        help="max number of hosts for map job",
    )
    parser.add_argument(
        "--reduce_hosts",
        "-rh",
        default=5,
        type=int,
        help="number of hosts for reduce task",
    )
    args = parser.parse_args()
    start_time0 = time.time()

    input_file = args.input
    script_file = args.script_path
    target_base_path = args.target_path
    hostsfile = args.hosts_file
    mb_size = args.split_size
    HINT_SIZE = mb_size * 1024 * 1000
    MAX_MAP_HOSTS = args.map_hosts
    MAX_REDUCE_HOSTS = args.reduce_hosts

    # format the msg to print
    msg = "[ MAPREDUCE STARTED ]"
    num_dots = 30 - int(len(msg) / 2)
    print("-" * num_dots + msg + "-" * num_dots)

    errors = []
    # add "/" at the end of base path if needed
    if not target_base_path.endswith("/"):
        target_base_path = target_base_path + "/"

    # get current hostname
    master_host, _ = exec_cmd(["hostname"], SSH_TIMEOUT)
    if DEBUG:
        print(f"[INIT] Got master hostname in {time.time()-start_time0:.2f}s")

    # list that will contain available hosts
    with open(hostsfile) as f:
        # get list of hosts
        # we dont take last element because it's empty
        hosts_list = f.read().split("\n")[:-1]

    # STEP 1: CHECK WORKERS STATUS
    # first loop to get the list of hosts that are up
    ssh_time0 = time.time()
    print(f"[INIT] Testing ssh connection on {len(hosts_list)} hosts...")
    i = 0
    hosts_up = []

    args_list = []
    for host in hosts_list:
        args_list.append((host, target_base_path))

    with Pool(30, maxtasksperchild=1) as p:
        pool_res = p.starmap(test_host, args_list)

    for i, x in enumerate(pool_res):
        if x[0]["up"] is not None:
            hosts_up.append(x[0]["up"])
        else:
            errors.append(x[1])

    ssh_time = time.time() - ssh_time0
    print(f"[INIT] Found {len(hosts_up)} hosts available in {ssh_time:.2f}s")

    # STEP 2: SPLITS
    # setup path for splits files
    master_split_path = target_base_path + "master_splits/"
    split_path = target_base_path + "splits/"
    script_path = target_base_path
    map_path = target_base_path + "maps/"

    split_time0 = time.time()
    num_hosts = len(hosts_up)
    print("[SPLIT] Splitting input file into chunks...")

    # get file size
    # file_size = os.path.getsize(input_file)
    # if (HINT_SIZE > (file_size / 2)) & (file_size > 40_000_000):
    # HINT_SIZE = file_size // 4
    # elif file_size <= 40_000_000:
    # HINT_SIZE = 10_000_000

    # select candidate hosts for MAP
    map_hosts = random.sample(hosts_up, MAX_MAP_HOSTS)

    num_splits, splits = make_splits(
        input_file,
        master_split_path,
        size_hint=HINT_SIZE,
        hosts_list=map_hosts,
        split_path=split_path,
        master_host=master_host,
    )

    # build the list of hosts that received splits
    # aka the hosts that will do the MAP task
    selected_hosts = []
    for host in splits.keys():
        if len(splits[host]) > 0:
            selected_hosts.append(host)

    makesplit_time = time.time() - split_time0
    print(
        f"[SPLIT] Uploaded {num_splits} splits of approx "
        f"{HINT_SIZE/(1024*1000):.2f}Mb in {makesplit_time:.2f}s"
    )

    # filenames = ["S" + str(i) + ".txt" for i in range(num_splits)]

    tot_split_time = time.time() - split_time0

    # STEP 3: MAP PHASE
    # run MAP on selected hosts
    map_time0 = time.time()
    print("[MAP] Starting MAP job on workers...")
    threads = []
    task_begin = i
    args_list = []
    path_to_script_file = script_path + script_file
    for host in selected_hosts:
        for filename in splits[host].strip().split(" "):
            path_to_split_file = split_path + filename
            script_args = f"0 {path_to_split_file}"
            args_list.append((host, path_to_script_file, script_args))

    with Pool(50) as p:
        pool_res = p.starmap(run_remote_script, args_list)

    # check results for MAP SUCCESS
    success, _ = check_results(pool_res, args_list)

    # check if all threads succeeded
    if success == 1:
        print("----- MAP FINISHED -----")
        map_time = time.time() - map_time0
        print(
            f"[MAP] MAP Successful in {map_time:.2f}s on "
            f"{len(selected_hosts)} hosts"
        )
        if DEBUG:
            print(f"[MAP] {selected_hosts}")
    else:
        map_time = time.time() - map_time0
        erprint(f"MAP Failed duration: {map_time:.2f}s")
        exit(1)

    # STEP 4: SHUFFLE PHASE
    # Prepare slaves for Shuffle phase
    # send hostsfile to all working slaves
    print("[SHUFF] Preparing Shuffle phase...")
    shuff_time0 = time.time()

    # define the list of hosts for reduce task
    if MAX_REDUCE_HOSTS <= len(selected_hosts):
        reduce_hosts = random.sample(selected_hosts, MAX_REDUCE_HOSTS)
    else:
        remain_hosts = set(hosts_up) - set(selected_hosts)
        num_extra_hosts = MAX_REDUCE_HOSTS - len(selected_hosts)
        reduce_hosts = selected_hosts + random.sample(remain_hosts, num_extra_hosts)

    # we want to create a file hosts_up.txt to send to workers
    hosts_up_file = target_base_path + "hosts_up.txt"
    with open(hosts_up_file, "w") as hu:
        hu.write("\n".join(reduce_hosts))

    args_list = []
    # send file to workers
    for j, host in enumerate(selected_hosts):
        args_list.append((hosts_up_file, host, target_base_path, master_host))

    with Pool(20, maxtasksperchild=1) as p:
        pool_res = p.starmap(deploy_file, args_list)

    # checking for errors
    success, _ = check_results(pool_res, args_list)
    if success == 1:
        print(
            "[SHUFF] Sent hosts list to workers in "
            f"{time.time()-shuff_time0:.2f}s, ready for shuffling!"
        )
    else:
        erprint("Errors encountered in preparation, exiting")
        exit(1)

    # start the Shuffle task
    print("[SHUFF] Starting SHUFFLE job on workers...")
    args_list = []
    path_to_script_file = script_path + script_file
    for host in splits.keys():
        if len(splits[host]) > 0:
            for filename in splits[host].strip(" ").split():
                path_to_map_file = map_path + filename.replace("S", "UM", 1)
                script_args = f"1 {path_to_map_file}"
                args_list.append((host, path_to_script_file, script_args))

    with Pool(50) as p:
        pool_res = p.starmap(run_remote_script, args_list)

    # check results for SHUFFLE SUCCESS
    success, results = check_results(pool_res, args_list)
    if success == 1:
        shuff_rcv_hosts = []
        for host in selected_hosts:
            for res in results[host]["success"]:
                idx = res.find(":")
                shuff_rcv_hosts += re.sub(r"[,\]\[}{']", "", res[idx + 2 :]).split()
        print("----- SHUFFLE FINISHED -----")
        # remove duplicates
        shuff_rcv_hosts = set(shuff_rcv_hosts)
        shuff_time = time.time() - shuff_time0
        num_map_hosts = len(selected_hosts)
        num_reduce_hosts = len(shuff_rcv_hosts)
        print(
            f"[SHUFF] SHUFFLE Successful in {shuff_time:.2f}s on "
            f"{num_map_hosts} hosts"
        )
        print(f"[SHUFF] {num_reduce_hosts} hosts received shuffles")
        if DEBUG:
            print(f"[SHUFF] {shuff_rcv_hosts}")
    else:
        shuff_time = time.time() - shuff_time0
        erprint(f"[SHUFF] Something went wrong after {shuff_time:.2f}s, exiting")
        exit(1)

    # STEP 5: REDUCE PHASE
    # start reduce job on hosts that have `shufflesreceived` dirs
    reduce_time0 = time.time()
    print("[REDUCE] Starting REDUCE job on workers...")
    args_list = []
    shuffhost_list = []

    for shuff_host in shuff_rcv_hosts:
        path_to_script_file = script_path + script_file
        script_args = "2"
        args_list.append((shuff_host, path_to_script_file, script_args))
        shuffhost_list.append(shuff_host)

    with Pool(30, maxtasksperchild=1) as p:
        pool_res = p.starmap(run_remote_script, args_list)

    # check results for REDUCE SUCCESS
    success, _ = check_results(pool_res, args_list)

    # check if all threads succeeded
    if success == 1:
        print("----- REDUCE FINISHED -----")
        # remove duplicates
        # shuff_rcv_hosts = set(shuff_rcv_hosts)
        reduce_time = time.time() - reduce_time0
        print(
            f"[REDUCE] Reduce job successful in {reduce_time:.2f}s on "
            f"{len(shuff_rcv_hosts)} hosts"
        )
        if DEBUG:
            print(f"[REDUCE] {shuff_rcv_hosts}")
    else:
        reduce_time = time.time() - reduce_time0
        erprint(f"[REDUCE] Something went wrong after {reduce_time}s, exiting")
        exit(1)

    # now we need to gather content of reduce dirs from `shuff_rcv_hosts`
    reduce_path = target_base_path + "reduces/"
    print("[OUTPUT] Collecting Reduce output from workers...")
    # make allreduces dir on master to store all reduces from workers
    dl_time0 = time.time()
    allreduce_path = target_base_path + "allreduces/"
    exec_cmd(["mkdir", "-p", allreduce_path], SSH_TIMEOUT)
    os.chdir(allreduce_path)
    args_list = []

    if ZIP_REDUCE:
        dl_path = target_base_path
    else:
        dl_path = reduce_path

    for (
        j,
        reduce_host,
    ) in enumerate(shuff_rcv_hosts):
        cmd = [
            "sftp",
            "-b",
            target_base_path + "sftp_cmd.txt",
            f"{reduce_host}:{dl_path}",
        ]
        args_list.append((cmd, SCRIPT_TIMEOUT))

    with Pool(30, maxtasksperchild=1) as p:
        pool_res = p.starmap(exec_cmd, args_list)

    dl_time = time.time() - dl_time0
    print(f"[OUTPUT] Downloaded all reduces in {dl_time:.2f}s")

    if ZIP_REDUCE:
        print("[OUTPUT] Unzipping files...")
        os.chdir(allreduce_path)
        (_, _, filenames) = next(os.walk(allreduce_path))

        unzip_time0 = time.time()
        args_list = []
        with Pool(10, maxtasksperchild=1) as p:
            pool_res = p.map(unzip, filenames)
        print(f"[OUTPUT] Unzipped all files in {time.time()- unzip_time0:.2f}s")

    sort_time0 = time.time()
    final_output = ""
    (_, _, filenames) = next(os.walk(allreduce_path))
    output_dict = {}
    for file in filenames:
        with open(file) as f:
            for line in f:
                word, count = line.split()
                output_dict[word] = int(count.strip("\n"))
            # final_output += f.read() + "\n"

    # final_output = final_output.strip("\n")
    final_output = sorted(output_dict.items(), key=lambda kv: (-kv[1], kv[0]))
    sort_time = time.time() - sort_time0
    print(f"[OUTPUT] Sorted all outputs in {sort_time:.2f}s")

    write_time0 = time.time()
    text = ""
    with open("/tmp/canat-21/output.txt", "w", 100 * (2 ** 20)) as f:
        for w_c in final_output:
            text += f"{w_c[0]} {w_c[1]}" + "\n"
        f.write(text)
    write_time = time.time() - write_time0
    print(f"[OUTPUT] writing output file took {write_time:.2f}s")

    tot_time = time.time() - start_time0
    msg = f"[ MAPREDUCE FINISHED in {tot_time:.2f}s ]"
    num_dots = 30 - int(len(msg) / 2)
    print("-" * num_dots + msg + "-" * num_dots)

    # append time stats to csv file
    time_stats = (
        input_file
        + ","
        + str(args.split_size)
        + ","
        + str(num_map_hosts)
        + ","
        + str(num_reduce_hosts)
        + ","
    )
    time_stats += (
        str(tot_split_time)
        + ","
        + str(map_time)
        + ","
        + str(shuff_time)
        + ","
        + str(reduce_time)
    )
    time_stats += "," + str(dl_time + sort_time) + "," + str(tot_time) + "\n"
    with open("/tmp/canat-21/stats.csv", "a") as f:
        f.write(time_stats)
