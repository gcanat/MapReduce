"""
All the tasks done by the workers: MAP, SHUFFLE, REDUCE
This time with multiprocessing instead of threading
"""
import sys
import re
import subprocess as sub
from multiprocessing import Pool, Manager
import os
import zipfile
import shutil
import time

DEBUG = False
BASE_PATH = "/tmp/canat-20"
MKD_TIMEOUT = 10
SSH_TIMEOUT = 600
#MAX_SHUFF_HOST = 15
ZIP_REDUCE = False

reduces = {}
results = []
shuff_rcv_hosts = set()


def erprint(*args, **kwargs):
    """Small function to print to stderr"""
    print(*args, file=sys.stderr, **kwargs)


def java_hash(s: str) -> int:
    """Function to mimic java hashcode"""
    h = 0
    for c in s:
        h = (31 * h + ord(c)) & 0xFFFFFFFF
    return ((h + 0x80000000) & 0xFFFFFFFF) - 0x80000000


def send_remote_cmd(host, cmd, timeout):
    """Send shell command to remote host"""
    with sub.Popen(cmd, stdout=sub.PIPE, stderr=sub.PIPE, text=True, encoding="utf8") as proc:
        try:
            stdout, stderr = proc.communicate(timeout=timeout)
        except sub.TimeoutExpired:
            proc.kill()
            stdout = ""
            stderr = f"{cmd} timed out after {timeout}s on host {host}"
    return stdout.strip("\n"), stderr.strip("\n")


def exec_cmd(cmd, timeout):
    """Execute shell command on local host"""
    with sub.Popen(cmd, stdout=sub.PIPE, stderr=sub.PIPE, text=True, encoding='utf8') as proc:
        try:
            stdout, stderr = proc.communicate(timeout=timeout)
        except sub.TimeoutExpired:
            proc.kill()
            stdout = ""
            stderr = f"{cmd} timed out after {timeout}s"
    return stdout.strip("\n"), stderr.strip("\n")


def map(filename, map_path):
    """Map task"""
    mapping = []
    # find the number of the file
    m = re.search(r"([0-9]+)\.txt$", filename)
    if m is not None:
        file_num = m.group(1)
    # open the file and go through each line and each word
    with open(filename, errors="ignore") as f:
        for line in f:
            words = line.split()
            for w in words:
                mapping.append(f'{w} 1')

    # write to UM file
    map_file = map_path + "UM" + file_num + ".txt"
    with open(map_file, 'w', errors="ignore") as mf:
        mf.write("\n".join(mapping))
        print("MAP SUCCESS")


def write_shufffiles(hash_files, hostname, hosts_list, i):
    """
    Insert `word 1` in the file named after the host that is going to receive that file
    So that all words that are going to the same host will be grouped in the same file
    """
    shuffle_file = shuffle_path + hosts_list[i] + "/" + hostname + ".txt"
    text = ""
    for word in hash_files.keys():
        # hashed_w = java_hash(word)
        hashed_w = hash(word)
        host_num = hashed_w % len(hosts_list)
        if host_num == i:
            text += f'{word} {hash_files[word]}\n'
    with open(shuffle_file, 'w', 32*(2**20)) as sf:
        sf.write(text)


def write_shuff(filename, shuffle_path,  hostname, hosts_list, i):
    n = len(hosts_list)
    shuffle_file = shuffle_path + hosts_list[i] + "/" + hostname + ".txt"
    text = ""
    with open(filename, 'r') as f:
        for line in f:
            word = line.split()[0]
            hashed_w = hash(word)
            host_num = hashed_w % n
            if host_num == i:
                text += f'{word} 1\n'
    with open(shuffle_file, 'w', 32*(2*20)) as sf:
        sf.write(text)


def prep_shuffle(hostname, filename, shuffle_path, hosts_list):
    """Prepare shuffle phase by creating the hash files"""
    hash_files = {}

    if DEBUG:
        start = time.time()

    n = len(hosts_list)
    args_list = []

    for i in range(n):
        args_list.append((filename, shuffle_path,  hostname, hosts_list, i))
    with Pool(20) as p:
        pool_res = p.starmap(write_shuff, args_list)

    if DEBUG:
        end = time.time()
        print(f"Finished preparing shuffle files: {end-start}s")


def deploy_file(file, host, target_path, protocol='scp', source_host=None):
    """Upload `file` on `host` into `target_path`"""
    if host == source_host:
        cmd = ['cp', '-r', file, target_path]
        stdout, stderr = exec_cmd(cmd, SSH_TIMEOUT)
    else:
        if protocol == 'scp-r':
            cmd = ['scp', '-r', file, f'{host}:{target_path}{file}']
        elif protocol == 'scp':
            cmd = ['scp', file, f'{host}:{target_path}']
        elif protocol == 'sftp':
            os.chdir(BASE_PATH + '/shuffles/' + host)
            cmd = ['sftp', '-b', BASE_PATH +
                   '/sftp_cmd2.txt', f'{host}:{target_path}']
        else:
            erprint("Wrong protocol specified")
            exit(1)
        stdout, stderr = send_remote_cmd(host, cmd, SSH_TIMEOUT)
    if stderr != "":
        erprint(stderr)
    # else:
        # erprint(stderr)
    return stdout, stderr


def shuff_thread(shuffle_path, curr_host, dirname, shuff_rcv_path):
    """Part of the shuffling that can be run in parallel: uploading files to hosts"""
    filename = dirname + "/" + curr_host + ".txt"

    stdout, stderr = deploy_file(filename, dirname, shuff_rcv_path,
                                 protocol='scp', source_host=curr_host)
    return stdout, stderr


def do_shuffle(curr_host, hostfile, shuffle_path):
    """For each file in shuffle_path/host/, send the file to host"""
    # get the host lists from file
    if DEBUG:
        print("Starting Shuffle phase...")
    with open(hostfile) as hf:
        hosts_list = hf.read().split("\n")
        if hosts_list[-1] == "":
            hosts_list = hosts_list[:-1]

    # get the filenames from shuffle path
    shuff_rcv_path = os.path.join(BASE_PATH, "shufflesreceived/")
    (_, dirnames, _) = next(os.walk(shuffle_path))

    os.chdir(shuffle_path)

    args_list = []
    for dirname in dirnames:
        args_list.append((shuffle_path, curr_host, dirname, shuff_rcv_path))

    if DEBUG:
        start = time.time()

    with Pool(12, maxtasksperchild=1) as p:
        pool_res = p.starmap(shuff_thread, args_list)

    if DEBUG:
        end = time.time()
        print(f"Sending shuffles took {end-start}s")
    # check if all went well
    all_errors = ["".join(x[1]) for x in pool_res]
    nb_errors = sum([len(x) for x in all_errors])
    if nb_errors == 0:
        print("SHUFFLE SUCCESS")
        print(f"Selected shuffle receive hosts: {dirnames}")
    else:
        erprint(f'Errors during shuffling: {all_errors}')


def reduce_thread(word_map, word, reduce_path, reduces):
    """Compute the reduce operation for a given word"""
    try:
        # counts = [int(x.split()[1]) for x in word_map]
        # reduced_count = sum(counts)
        reduced_count = sum(word_map)
        reduces[word] = reduced_count
        stdout = "REDUCE SUCCESS"
        stderr = ""
    except Exception as e:
        erprint(f"Error on word {word}: {repr(e)}")
        stdout = ""
        stderr = f"Error on word {word}: {repr(e)}"
    return stdout, stderr


def reduce(shuff_rcv_path, curr_host):
    """
    Create a file named <hash key>.txt in the directory
    `<target base path>/reduces/`
    """
    all_words = {}
    reduce_path = os.path.join(BASE_PATH, "reduces/")
    cmd = ["mkdir", "-p", reduce_path]
    _, curr_error = exec_cmd(cmd, MKD_TIMEOUT)
    # if no error we can continue
    if curr_error != "":
        erprint(f"Failed to create maps directory: {curr_error}")
        erprint("Exiting")
        exit(1)

    (shuff_path, _, filenames) = next(os.walk(shuff_rcv_path))

    # reading all files in shufflesreceived and populating a dictionnary of
    # words with the list of corresponding lines
    if DEBUG:
        start = time.time()
        print("Starting reading shuffreceived files")
    for file in filenames:
        with open(os.path.join(shuff_path, file)) as f:
            file_content = f.read().split("\n")
            if file_content[-1] == "":
                file_content = file_content[:-1]
            for line in file_content:
                word, w_count = line.split()[0:2]
                if word in all_words.keys():
                    all_words[word] += int(w_count)
                else:
                    all_words[word] = int(w_count)

    if DEBUG:
        print(f"Done in {time.time()-start}s")
        start = time.time()
        print("Starting reducing")
    # for i, word in enumerate(all_words.keys()):
        # args_list.append((all_words[word], word, reduce_path, reduces))
    # with Pool(100) as p:
        # pool_res = p.starmap(reduce_thread, args_list)
    reduces = sorted(all_words.items(), key=lambda kv: (-kv[1], kv[0]))
    pool_res = ""

    if DEBUG:
        print(f"Done in {time.time()-start}s")
        print("Writing to reduce file")
        start = time.time()

    # generate the text that will be written in the reduce file
    text = ""
    for w_c in reduces:
        text += f"{w_c[0]} {w_c[1]}\n"
    # text = text.strip("\n")
    # write the reduce file
    with open(reduce_path + "reduces-" + curr_host + ".txt", 'w', 32*(2**20)) as f:
        f.write(text)

    if DEBUG:
        print(f"Done writing reduce file in {time.time()-start}s")

    # zip the whole directory
    if ZIP_REDUCE:
        os.chdir(BASE_PATH)
        shutil.make_archive(f'{curr_host}', 'zip', 'reduces/')
    # check if all went well
    all_errors = [x[1] for x in pool_res]
    nb_errors = sum([len(x) for x in all_errors])
    if nb_errors == 0:
        print("REDUCE SUCCESS")
    else:
        erprint(f'Errors during reduce: {all_errors}')


if __name__ == "__main__":
    allowed_modes = ["0", "1", "2"]
    if sys.argv[1] == "--help":
        print("Usage: python3 slave.py <mode> <filename>")
        print("example mode 0 (map phase): python3 slave.py 0 /tmp/canat-20/S0.txt")
        print(
            "example mode 1 (shuffle phase): python3 slave.py 1 /tmp/canat-20/maps/UM0.txt")
        print("example mode 2 (reduce phase): python3 slave.py 2")
        exit(0)
    if sys.argv[1] not in allowed_modes:
        erprint("Wrong mode argument supplied.")
        erprint("Supported modes: ", allowed_modes)
        erprint("Exiting.")
        exit(1)
    calc_mode = sys.argv[1]
    threads = []

    # mode "0": MAP
    if calc_mode == "0":
        filename = sys.argv[2]
        map_path = os.path.join(BASE_PATH, "maps/")
        # create map directory
        cmd = ["mkdir", "-p", map_path]
        _, curr_error = exec_cmd(cmd, MKD_TIMEOUT)
        # if no error we can continue
        if curr_error != "":
            erprint(f"Failed to create maps directory: {curr_error}")
            erprint("Exiting")
            exit(1)
        map(filename, map_path)

    # mode "1": prepare shuffle
    elif calc_mode == "1":
        filename = sys.argv[2]
        shuffle_path = os.path.join(BASE_PATH, "shuffles/")
        hosts_file = os.path.join(BASE_PATH, "hosts_up.txt")
        curr_host, curr_error = exec_cmd(['hostname'], MKD_TIMEOUT)
        if curr_error != "":
            erprint(f"Failed to gather worker's hostname: {curr_error}")
            erprint("Exiting.")
            exit(1)
        # on va créer un fichier par host de destination
        # on écrit dans ces fichiers les mots dont le hash match avec le host
        # (via modulo)
        # puis on peut uploader directement le fichier sur le host de destination
        with open(hosts_file) as hf:
            hosts_list = hf.read().split("\n")
            if hosts_list[-1] == "":
                hosts_list = hosts_list[:-1]

        # create shuffle path
        # _, stderr = exec_cmd(['mkdir', '-p', shuffle_path], MKD_TIMEOUT)
        # if stderr != "":
            # erprint(f"Failed to create shuffles directory: {curr_error}")
            # erprint("Exiting.")
            # exit(1)

        # create host dirs inside shuffle path in parallel
        args_list = []
        for host in hosts_list:
            cmd = ['mkdir', '-p', shuffle_path + host]
            args_list.append((cmd, MKD_TIMEOUT))
        with Pool(12) as p:
            pool_res = p.starmap(exec_cmd, args_list)

        for (stdout, stderr) in pool_res:
            if stderr != "":
                erprint(f"Failed to create shuffles directory: {curr_error}")
                erprint("Exiting.")
                exit(1)
        prep_shuffle(curr_host, filename, shuffle_path, hosts_list)
        do_shuffle(curr_host, hosts_file, shuffle_path)
        #

    elif calc_mode == "2":
        curr_host, curr_error = exec_cmd(['hostname'], MKD_TIMEOUT)
        if curr_error != "":
            erprint(f"Failed to gather worker's hostname: {curr_error}")
            erprint("Exiting.")
            exit(1)
        reduce(os.path.join(BASE_PATH, f"shufflesreceived/"), curr_host)
