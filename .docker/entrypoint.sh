#!/bin/bash

COLOR_GREEN=$(tput setaf 2 2>/dev/null)
COLOR_BLUE=$(tput setaf 4 2>/dev/null)
COLOR_MAGENTA=$(tput setaf 5 2>/dev/null)
COLOR_DEFAULT=$(tput sgr0 2>/dev/null)

EXEC_PATH=$(dirname $(readlink -f "$0"))
HOME_PATH=$EXEC_PATH/..
SRC_PATH=$HOME_PATH/src
PYTHON_DIST_PACKAGES_PATH=/usr/local/lib/python3.13/site-packages
STEP=1
STEP_COUNT=5

# Memory check: 3 ZooKeeper, 2 NameNodes, 3 workers and the YARN containers need ~6 GB
MEMORY_GB=$(awk '/MemTotal/ {printf "%d", $2 / 1048576}' /proc/meminfo)
if [ "$MEMORY_GB" -lt 9 ]; then
  echo "${COLOR_MAGENTA}WARNING:${COLOR_DEFAULT} Docker has ${MEMORY_GB} GB of memory. Give Docker at least 10 GB: Docker Desktop > Settings > Resources > Memory."
fi

# Generate home path file and add it to Python search path
echo "${COLOR_MAGENTA}${STEP}/$STEP_COUNT:${COLOR_GREEN} Generating ${COLOR_BLUE}hadoop-hdfs-yarn-architecture.pth${COLOR_GREEN} file and adding it to Python search path.${COLOR_DEFAULT}"
echo "$(readlink -f $SRC_PATH)" | sudo tee $PYTHON_DIST_PACKAGES_PATH/hadoop-hdfs-yarn-architecture.pth
let STEP++

# Install Python requirements.txt once. If requirements are satisfied, installation is skipped
pip freeze | sort > /tmp/installed.txt
sed -E 's/#.*//; s/^[[:space:]]+//; s/[[:space:]]+$//' "$HOME_PATH/requirements.txt" \
  | sed '/^$/d' \
  | sort > /tmp/required.txt
MISSING_LINES=$(comm -23 /tmp/required.txt /tmp/installed.txt | wc -l)
if [ "$MISSING_LINES" -eq 0 ]; then
  echo "${COLOR_MAGENTA}${STEP}/$STEP_COUNT:${COLOR_GREEN} Python requirements already satisfied. Skipping installation.${COLOR_DEFAULT}"
else
  echo "${COLOR_MAGENTA}${STEP}/$STEP_COUNT:${COLOR_GREEN} Installing Python requirements.txt dependencies...${COLOR_DEFAULT}"
  sudo pip install --upgrade --upgrade-strategy only-if-needed -r "$HOME_PATH/requirements.txt"
fi
rm /tmp/installed.txt /tmp/required.txt
let STEP++

# HDFS: one active NameNode, out of safe mode, with the 3 DataNodes
echo "${COLOR_MAGENTA}${STEP}/$STEP_COUNT:${COLOR_GREEN} Waiting for ${COLOR_BLUE}HDFS${COLOR_GREEN} (active NameNode out of safe mode, 3 DataNodes)...${COLOR_DEFAULT}"
until hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF"; do
  sleep 2
done
until [ "$(hdfs dfsadmin -report -live 2>/dev/null | grep -c '^Name:')" -ge 3 ]; do
  sleep 2
done
hdfs haadmin -getAllServiceState 2>/dev/null
hdfs dfs -mkdir -p /user/hadoop /tmp
hdfs dfs -chmod 1777 /tmp
let STEP++

# YARN: one active ResourceManager and the 3 NodeManagers
echo "${COLOR_MAGENTA}${STEP}/$STEP_COUNT:${COLOR_GREEN} Waiting for ${COLOR_BLUE}YARN${COLOR_GREEN} (active ResourceManager, 3 NodeManagers)...${COLOR_DEFAULT}"
until [ "$(yarn node -list 2>/dev/null | grep -c RUNNING)" -ge 3 ]; do
  sleep 2
done
yarn rmadmin -getAllServiceState 2>/dev/null
let STEP++

# JobHistory: MapReduce jobs need it to report their final state
echo "${COLOR_MAGENTA}${STEP}/$STEP_COUNT:${COLOR_GREEN} Waiting for ${COLOR_BLUE}JobHistory${COLOR_GREEN}...${COLOR_DEFAULT}"
until curl -sf "$JOBHISTORY_HTTP/ws/v1/history/info" >/dev/null; do
  sleep 2
done

echo "${COLOR_GREEN}Ready. Next step: ${COLOR_BLUE}python lessons/00_seed.py${COLOR_DEFAULT}"
