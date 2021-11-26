#!/bin/zsh
echo "[CLEANUP] cleaning local dirs"
rm -rf /tmp/canat-20/allreduces/
rm -rf /tmp/canat-20/master_splits/
echo "[CLEANUP] local dirs cleaned!"
#python3 clean.py hosts.txt /tmp/canat-20/
python3 deploy.py slave.py hosts.txt /tmp/canat-20
python3 deploy.py sftp_cmd.txt hosts.txt /tmp/canat-20
#python3 deploy.py sftp_cmd2.txt hosts.txt /tmp/canat-20
export PYTHONHASHSEED=0
python3 master.py -i $1 -sp slave.py -sz $2 -mh $3 -rh $4

echo "[CLEANUP] cleaning local dirs"
rm -rf /tmp/canat-20/allreduces/
rm -rf /tmp/canat-20/master_splits/
echo "[CLEANUP] local dirs cleaned!"
python3 clean.py hosts.txt /tmp/canat-20/
