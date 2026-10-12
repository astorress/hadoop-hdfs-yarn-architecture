#!/bin/bash
# Arranque de un nodo: corre en primer plano los demonios de $DAEMONS (separados por espacios).
#   journalnode  edits compartidos del NameNode (nn1, nn2, worker1)
#   namenode     metadatos de HDFS (nn1 formatea el clúster; nn2 se clona de nn1 con -bootstrapStandby)
#   zkfc         vigila a su NameNode y pelea el lock de ZooKeeper para ser el activo
#   resourcemanager, nodemanager, datanode, historyserver
set -e

wait_for() {  # host puerto
  until (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null; do sleep 2; done
}

init_namenode() {
  if [ -f /data/namenode/current/VERSION ]; then
    # ZooKeeper data may be newer/older than the persistent NameNode volume. Reconcile the HA znode on every nn1 boot.
    if [ "$HOSTNAME" == "nn1" ]; then
      hdfs zkfc -formatZK -nonInteractive || true
    fi
    return
  fi
  for jn in nn1 nn2 worker1; do wait_for $jn 8485; done
  if [ "$HOSTNAME" == "nn1" ]; then
    echo "Formateando el clúster (primera vez)"
    hdfs namenode -format -nonInteractive -clusterId hadoop-hdfs-yarn-architecture
    hdfs zkfc -formatZK -nonInteractive || true   # nodo /hadoop-ha/lake en ZooKeeper
  else
    echo "Clonando los metadatos de nn1 (bootstrapStandby)"
    wait_for nn1 9870
    until hdfs namenode -bootstrapStandby -nonInteractive; do sleep 3; done
  fi
}

for daemon in $DAEMONS; do
  case $daemon in
    journalnode)     hdfs journalnode & ;;
    namenode)        touch /hadoop-admin/dfs.exclude; init_namenode; hdfs namenode & ;;
    zkfc)            hdfs zkfc & ;;
    datanode)        hdfs datanode & ;;
    resourcemanager) yarn resourcemanager & ;;
    nodemanager)     yarn nodemanager & ;;
    historyserver)   mapred historyserver & ;;
  esac
done

# Si un demonio termina, el contenedor también, y Docker lo reinicia (restart: unless-stopped)
wait -n
exit 1
