# Lección 04 - Réplicas en el tiempo: setrep, decommission de un DataNode, re-replicación y balancer.
#
# El NameNode compara sin parar las réplicas que tiene cada bloque con las que debería tener. Si faltan (un DataNode
# murió o sale del clúster), ordena copiarlas; si sobran, ordena borrar las de más. Sacar un nodo de forma ordenada se
# llama decommission: sus bloques se copian a otros nodos antes de apagarlo, así nunca hay un momento con menos copias.
import re
import time

from lab.config import EXCLUDE_FILE, RAW, WORK
from lab.hdfs import active_namenode, datanodes, fsck_blocks, hdfs, jmx, trigger_block_reports
from lab.show import step, table, title

DEMO = f"{WORK}/04"
NODE = "worker3"  # el único DataNode del rack-b

title("04 - Replicación, decommission y balancer")


def refresh(excluded):
    with open(EXCLUDE_FILE, "w") as exclude:
        exclude.write("".join(f"{host}\n" for host in excluded))
    return hdfs("dfsadmin", "-refreshNodes")  # cada NameNode vuelve a leer dfs.exclude


def health(path="/"):
    output = hdfs("fsck", path)
    fields = ("Total blocks \\(validated\\)", "Under-replicated blocks", "Over-replicated blocks", "Mis-replicated blocks")
    return {f.split(" ")[0].split("\\")[0].lower() + "_" + f.split(" ")[1].lower().strip("(\\"):
            int(re.search(rf"{f}:\s+(\d+)", output).group(1)) for f in fields} | {
                "status": re.search(r"Status: (\w+)", output).group(1)}


def blocks_per_node():
    return {n["datanode"]: f"{n['blocks']} ({n['admin_state']})" for n in datanodes()}


refresh([])
trigger_block_reports()  # por si hubo un failover antes (lección 03): el activo debe conocer todas las réplicas
hdfs("dfs", "-rm", "-r", "-f", "-skipTrash", DEMO)
hdfs("dfs", "-mkdir", "-p", DEMO)

step("Los DataNodes y cuántos bloques guarda cada uno (con replicación 3 y 3 nodos, todos tienen todo)")
table(datanodes())

step("setrep: bajar la replicación de un archivo deja réplicas de más; subirla, réplicas de menos")
hdfs("dfs", "-D", "dfs.blocksize=1m", "-cp", f"{RAW}/readings/readings_2026-09-02.csv", f"{DEMO}/day.csv")
hdfs("dfs", "-setrep", "-w", "3", f"{DEMO}/day.csv")
for replication in ("1", "3"):
    hdfs("dfs", "-setrep", replication, f"{DEMO}/day.csv")  # sin -w: no espera
    print(f"setrep {replication} recién aplicado ->", health(f"{DEMO}/day.csv"))
    hdfs("dfs", "-setrep", "-w", replication, f"{DEMO}/day.csv")
    print(f"setrep {replication} terminado       ->", health(f"{DEMO}/day.csv"))
table(fsck_blocks(f"{DEMO}/day.csv"))

step(f"Decommission de {NODE}: se agrega a dfs.exclude y los NameNodes vuelven a leer la lista")
print(refresh([NODE]))
for _ in range(6):
    time.sleep(5)
    decom = jmx(active_namenode(), "Hadoop:service=NameNode,name=NameNodeInfo")["DecomNodes"]
    waiting = re.search(r"underReplicatedBlocks.:(\d+)", decom).group(1)
    print(f"  {time.strftime('%H:%M:%S')}  {NODE}: {next(n['admin_state'] for n in datanodes() if n['datanode'] == NODE)}"
          f" · bloques que esperan otra réplica: {waiting}")
print(f"El decommission no avanza: cada bloque debe quedar con 3 réplicas fuera de {NODE}, pero solo quedan 2")
print("DataNodes. HDFS nunca baja voluntariamente de dfs.replication. Hay dos salidas: agregar un nodo, o aceptar")
print("menos copias. Aquí se baja la replicación de los datos a 2.")

step("setrep -R 2 sobre todo el sistema de archivos: ahora el decommission puede terminar")
started = time.monotonic()
# Sin -w: un bloque sigue listando la réplica del nodo en decommission, así que nunca "tendría" exactamente 2
hdfs("dfs", "-setrep", "-R", "2", "/")  # todo: basta un bloque con 3 réplicas para trabarlo
while next(n["admin_state"] for n in datanodes() if n["datanode"] == NODE) != "Decommissioned":
    time.sleep(2)
print(f"{NODE} quedó Decommissioned en {time.monotonic() - started:.0f} s")
table(datanodes())
print(f"{NODE} sigue vivo y conserva sus bloques, pero ya no cuenta como réplica y no recibe escrituras.")
print("Todos los bloques quedaron en el rack-a: con un solo rack disponible, la regla de racks no se puede cumplir.")
print(health("/data"))

step(f"Mientras {NODE} está fuera, se escriben datos nuevos (solo pueden ir a worker1 y worker2)")
hdfs("dfs", "-D", "dfs.blocksize=1m", "-cp", f"{RAW}/readings/readings_2026-09-03.csv", f"{DEMO}/new.csv")
table(fsck_blocks(f"{DEMO}/new.csv")[:3])

step(f"Recommission: {NODE} sale de dfs.exclude y vuelve a recibir réplicas; la replicación vuelve a 3")
print(refresh([]))
time.sleep(5)
started = time.monotonic()
hdfs("dfs", "-setrep", "-R", "-w", "3", "/")
print(f"re-replicación a 3 copias completa en {time.monotonic() - started:.0f} s")
print("bloques por DataNode:", blocks_per_node())
table(fsck_blocks(f"{DEMO}/new.csv")[:3])
print(health("/data"))
print(f"Los bloques nuevos solo necesitaban una tercera copia: el NameNode la puso en {NODE}, el único en otro rack.")

step("Balancer: mueve bloques entre DataNodes hasta que el uso de cada uno esté a ±1 % del promedio")
output = hdfs("balancer", "-threshold", "1", check=False)
print("\n".join(line for line in output.splitlines()[-4:]))
print("Con 3 nodos que guardan todo (replicación 3) el clúster ya está balanceado. En un clúster real, al agregar")
print("un nodo vacío el balancer le pasa bloques de los llenos; corre en segundo plano y con ancho de banda limitado")
print("(dfs.datanode.balance.bandwidthPerSec) para no afectar a los trabajos.")
hdfs("dfs", "-rm", "-r", "-skipTrash", DEMO)
print("\nSiguiente: python lessons/05_storage_features.py")
