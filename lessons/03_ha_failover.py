# Lección 03 - Failover de HDFS en vivo: el NameNode activo cambia mientras dos clientes siguen escribiendo.
#
# hdfs haadmin -failover pide a los ZKFC un cambio ordenado: el activo pasa a standby, suelta el lock de ZooKeeper y
# el otro lo toma, lee los últimos edits de los JournalNodes y se vuelve activo. Los clientes apuntan a "hdfs://lake":
# cuando el NameNode al que hablan responde StandbyException, el proxy de failover prueba con el otro y reintenta.
import re
import subprocess
import tempfile
import threading
import time

from kazoo.client import KazooClient

from lab.config import WORK, ZOOKEEPER
from lab.hdfs import active_namenode, hdfs, jmx, namenode_states, trigger_block_reports
from lab.show import step, table, title

DEMO = f"{WORK}/03"

title("03 - Failover de HDFS")
hdfs("dfs", "-rm", "-r", "-f", "-skipTrash", DEMO)
hdfs("dfs", "-mkdir", "-p", DEMO)


def lock_owner():
    zk = KazooClient(hosts=ZOOKEEPER, read_only=True)
    zk.start()
    data, _ = zk.get("/hadoop-ha/lake/ActiveStandbyElectorLock")
    zk.stop()
    return re.findall(rb"nn\d", data)[-1].decode()


def epoch():
    return jmx("worker1", "Hadoop:service=JournalNode,name=Journal-lake", port=8480).get("LastPromisedEpoch")


step("Antes del failover")
table(namenode_states())
first = active_namenode()
second = next(r["namenode"] for r in namenode_states() if r["namenode"] != first)
print(f"lock de ZooKeeper: {lock_owner()} · epoch de los JournalNodes: {epoch()}")

step("Dos escritores: cada uno sube archivos sin parar con 'hdfs dfs -put' (un cliente Java por archivo)")
results = []
stop = threading.Event()
payload = tempfile.NamedTemporaryFile(suffix=".csv")
payload.write(b"meter_id,ts,kwh\nM00001,2026-09-30 00:00:00,0.017\n")
payload.flush()


def writer(name):
    i = 0
    while not stop.is_set():
        started = time.monotonic()
        result = subprocess.run(["hdfs", "dfs", "-put", payload.name, f"{DEMO}/{name}-{i:03d}.csv"],
                                capture_output=True, text=True)
        results.append({"writer": name, "file": i, "at": started, "seconds": time.monotonic() - started,
                        "ok": result.returncode == 0, "error": result.stderr.strip()[-120:]})
        i += 1


threads = [threading.Thread(target=writer, args=(f"w{n}", )) for n in (1, 2)]
for thread in threads:
    thread.start()
time.sleep(6)

step(f"hdfs haadmin -failover {first} {second} (con los escritores corriendo)")
failover_at = time.monotonic()
print(hdfs("haadmin", "-failover", first, second))
print(f"duración del failover: {time.monotonic() - failover_at:.1f} s")
time.sleep(8)
stop.set()
for thread in threads:
    thread.join()

step("Lo que vieron los escritores")
before = [r for r in results if r["at"] + r["seconds"] < failover_at]
during = [r for r in results if r["at"] < failover_at + 3 and r["at"] + r["seconds"] >= failover_at]
after = [r for r in results if r["at"] >= failover_at + 3]
table([{
    "phase": phase,
    "puts": len(rows),
    "ok": sum(r["ok"] for r in rows),
    "failed": sum(not r["ok"] for r in rows),
    "max_seconds": round(max((r["seconds"] for r in rows), default=0), 2),
} for phase, rows in (("antes", before), ("durante", during), ("después", after))])
for failure in [r for r in results if not r["ok"]][:3]:
    print("error:", failure["error"])
written = len(hdfs("dfs", "-ls", "-C", DEMO).splitlines())
print(f"archivos en HDFS: {written} · puts exitosos: {sum(r['ok'] for r in results)}")
print("Un cliente que habla con un NameNode en standby recibe StandbyException; el proxy de failover")
print("(ConfiguredFailoverProxyProvider) espera un momento y reintenta con el otro. Por eso el failover solo se nota")
print("como algo más de latencia, no como errores.")

step("Después del failover")
table(namenode_states())
print(f"lock de ZooKeeper: {lock_owner()} · epoch de los JournalNodes: {epoch()}")
print("El epoch subió: si el NameNode anterior intentara escribir un edit, los JournalNodes lo rechazarían.")

step(f"Volver a dejar a {first} como activo")
print(hdfs("haadmin", "-failover", second, first))
print(hdfs("haadmin", "-getAllServiceState"))
print("Un failover no planeado (el nodo activo muere) lo hace el ZKFC solo, sin haadmin: ver Experimentos en el README.")

step("Lo que un failover deja pendiente: réplicas sobrantes que el nuevo activo todavía no se atreve a borrar")
postponed = jmx(first, "Hadoop:service=NameNode,name=FSNamesystem")["PostponedMisreplicatedBlocks"]
print(f"bloques con decisión pospuesta en {first}: {postponed}")
print("El nuevo activo no sabe si su lista de réplicas está al día (el anterior pudo ordenar borrados que él no vio).")
print("Hasta que cada DataNode le envía un reporte completo de bloques, no borra réplicas sobrantes. Los DataNodes")
print("lo envían cada 6 horas (dfs.blockreport.intervalMsec); triggerBlockReport lo pide ahora mismo.")
trigger_block_reports()
hdfs("dfs", "-rm", "-r", "-skipTrash", DEMO)
print("\nSiguiente: python lessons/04_replication_rack.py")
