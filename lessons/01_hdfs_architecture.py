# Lección 01 - Arquitectura de HDFS en alta disponibilidad: nameservice, NameNodes, JournalNodes, ZooKeeper y racks.
#
# Un solo NameNode es un punto único de falla: sin él no se puede leer ni escribir nada. Con alta disponibilidad hay
# dos: el activo atiende a los clientes y el standby se mantiene al día leyendo los cambios (edits) que el activo
# escribe en los JournalNodes. Un ZKFC por NameNode vigila su salud y, con un lock en ZooKeeper, decide quién es el
# activo. Los clientes no apuntan a un host sino al nameservice "lake" y prueban los NameNodes hasta dar con el activo.
import re
from collections import Counter

import requests
from kazoo.client import KazooClient

from lab.config import NAMENODE_HTTP_PORT, NAMENODES, RAW, WORK, ZOOKEEPER
from lab.hdfs import datanodes, fsck_blocks, hdfs, jmx, namenode_states
from lab.show import step, table, title

DAY_FILE = f"{RAW}/readings/readings_2026-09-01.csv"
DEMO = f"{WORK}/01"

title("01 - Arquitectura de HDFS")

step("El nameservice: cómo resuelve un cliente 'hdfs://lake' (hdfs getconf lee /hadoop/conf)")
print("fs.defaultFS:       ", hdfs("getconf", "-confKey", "fs.defaultFS"))
print("NameNodes:          ", hdfs("getconf", "-namenodes"))
print("JournalNodes:       ", hdfs("getconf", "-journalNodes"))
print("proxy de failover:  ", hdfs("getconf", "-confKey", "dfs.client.failover.proxy.provider.lake").split(".")[-1])

step("Estado de cada NameNode: uno activo y uno standby (hdfs haadmin y JMX)")
print(hdfs("haadmin", "-getAllServiceState"))
table(namenode_states())
print("last_txid: el último cambio que conoce cada uno. El standby lee los edits cada 5 s (dfs.ha.tail-edits.period),")
print("así que puede ir unos pocos cambios detrás. Los DataNodes reportan sus bloques a los dos NameNodes.")

step("El standby no atiende clientes: WebHDFS contra cada NameNode")
for nn in NAMENODES:
    response = requests.get(f"http://{nn}:{NAMENODE_HTTP_PORT}/webhdfs/v1{RAW}",
                            params={"op": "GETFILESTATUS", "user.name": "hadoop"}, timeout=5)
    detail = response.json().get("RemoteException", {}).get("exception", "OK") if response.content else ""
    print(f"{nn}: HTTP {response.status_code} {detail}")

step("JournalNodes: el activo escribe cada edit en la mayoría (2 de 3); el standby los lee de ahí")
rows = []
for jn in ("nn1", "nn2", "worker1"):
    journal = jmx(jn, "Hadoop:service=JournalNode,name=Journal-lake", port=8480)
    rows.append({"journalnode": jn, "last_written_txid": journal.get("LastWrittenTxId"),
                 "lag_txns": journal.get("CurrentLagTxns"), "last_promised_epoch": journal.get("LastPromisedEpoch")})
table(rows)
print("epoch: sube en cada cambio de activo. Un JournalNode rechaza escrituras con un epoch viejo: así un NameNode")
print("que perdió el rol (pero no lo sabe) no puede corromper el historial (fencing por quórum).")

step("ZooKeeper: el lock que tiene el ZKFC del NameNode activo (y el del ResourceManager activo)")
zk = KazooClient(hosts=ZOOKEEPER, read_only=True)
zk.start()
for path in ("/hadoop-ha/lake/ActiveStandbyElectorLock", "/yarn-leader-election/meter-yarn/ActiveStandbyElectorLock"):
    data, stat = zk.get(path)
    holder = re.findall(rb"(nn\d|rm\d)", data)
    print(f"{path}\n   dueño: {holder[-1].decode() if holder else data}  (nodo efímero de la sesión {stat.ephemeralOwner:#x})")
print("hijos de /hadoop-ha/lake:", zk.get_children("/hadoop-ha/lake"))
zk.stop()
print("Es un nodo efímero: si el ZKFC del activo pierde su sesión (el nodo muere), el nodo desaparece y el otro ZKFC")
print("lo crea y promueve a su NameNode. ActiveBreadCrumb deja dicho quién fue el último activo, para aislarlo.")

step("DataNodes y racks: el NameNode pregunta a /hadoop/topology.sh en qué rack está cada uno")
table(datanodes())
print(hdfs("dfsadmin", "-printTopology"))

step("Colocación de réplicas: el mismo archivo con bloques de 1 MB y replicación 3")
hdfs("dfs", "-rm", "-r", "-f", "-skipTrash", DEMO)
hdfs("dfs", "-mkdir", "-p", DEMO)
hdfs("dfs", "-D", "dfs.blocksize=1m", "-cp", DAY_FILE, f"{DEMO}/day_1mb.csv")
# Basta con que el pipeline escriba 1 réplica (dfs.namenode.replication.min) para cerrar el archivo; si quedó alguna
# por escribir, el NameNode la completa en segundo plano. -setrep -w espera a que estén las 3.
hdfs("dfs", "-setrep", "-w", "3", f"{DEMO}/day_1mb.csv")
blocks = fsck_blocks(f"{DEMO}/day_1mb.csv")
table(blocks)
racks = Counter(len(set(re.findall(r"\((/[\w-]+)\)", b["where"]))) for b in blocks)
print(f"bloques repartidos en 2 racks: {racks[2]} de {len(blocks)}")
print("Política por defecto: 1ª réplica en el nodo del escritor (o uno al azar), 2ª en otro rack y 3ª en el mismo rack")
print("que la 2ª. Así un rack entero puede caer sin perder datos, y solo una copia cruza entre racks.")

step("Con replicación 2, las dos copias de cada bloque siempre quedan en racks distintos")
hdfs("dfs", "-D", "dfs.blocksize=1m", "-D", "dfs.replication=2", "-cp", DAY_FILE, f"{DEMO}/day_rep2.csv")
hdfs("dfs", "-setrep", "-w", "2", f"{DEMO}/day_rep2.csv")
table(fsck_blocks(f"{DEMO}/day_rep2.csv"))

step("Capacidad del clúster (hdfs dfsadmin -report)")
report = hdfs("dfsadmin", "-report")
print("\n".join(line for line in report.splitlines()[:12] if line.strip()))
print("\nSiguiente: python lessons/02_namenode_metadata.py")
