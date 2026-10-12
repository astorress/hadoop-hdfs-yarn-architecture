# Lección 06 - YARN: ResourceManagers en HA, NodeManagers, colas del Capacity Scheduler y preemption.
#
# YARN reparte memoria y CPU entre aplicaciones. El Capacity Scheduler organiza el clúster en colas con una capacidad
# garantizada: prod tiene el 70 % y dev el 30 %. Una cola puede crecer más allá de lo suyo si el resto está libre
# (elasticidad); cuando la dueña de esos recursos los necesita, la preemption mata contenedores de la que se excedió.
import subprocess
import time

from lab.config import WORK
from lab.hdfs import active_resourcemanager, hdfs, queues, run, yarn_api, yarn_nodes
from lab.show import step, table, title

DEMO = f"{WORK}/06"

title("06 - YARN: colas y preemption")

step("Los dos ResourceManagers: uno activo, uno standby (el estado de las aplicaciones vive en ZooKeeper)")
print(run("yarn", "rmadmin", "-getAllServiceState"))
info = yarn_api("info")["clusterInfo"]
print(f"activo: {active_resourcemanager()} · versión {info['hadoopVersion']} · arrancó {time.ctime(info['startedOn'] / 1000)}")

step("Los NodeManagers: cada uno ofrece 1,5 GB y 2 vcores para contenedores")
table(yarn_nodes())
metrics = yarn_api("metrics")["clusterMetrics"]
print(f"clúster: {metrics['totalMB']} MB y {metrics['totalVirtualCores']} vcores en {metrics['activeNodes']} nodos")

step("Las colas (Capacity Scheduler): capacidad garantizada y máxima de cada una")
table(queues())
print("Un map de MapReduce pide 384 MB; un ApplicationMaster, 512 MB. Caben unos 10 contenedores a la vez.")


def sleep_job(queue, maps, seconds):
    """Un trabajo map-only cuyos maps solo esperan: sirve para ocupar contenedores durante un tiempo conocido.
    NLineInputFormat crea un map por cada línea de la entrada."""
    hdfs("dfs", "-rm", "-r", "-f", "-skipTrash", f"{DEMO}/{queue}")
    hdfs("dfs", "-put", "-f", "-", f"{DEMO}/{queue}.txt", input="".join(f"{i}\n" for i in range(maps)))
    return subprocess.Popen(["mapred", "streaming",
                             "-D", f"mapreduce.job.queuename={queue}",
                             "-D", f"mapreduce.job.name=sleep-{queue}",
                             "-D", "mapreduce.input.lineinputformat.linespermap=1",
                             "-files", "lessons/mapreduce/sleep_mapper.py",
                             "-inputformat", "org.apache.hadoop.mapred.lib.NLineInputFormat",
                             "-input", f"{DEMO}/{queue}.txt", "-output", f"{DEMO}/{queue}",
                             "-mapper", f"python3 sleep_mapper.py {seconds}", "-numReduceTasks", "0"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def snapshot(label):
    rows = {q["queue"]: q for q in queues()}
    print(f"  {time.strftime('%H:%M:%S')} {label:28} dev: {rows['dev']['containers']:>2} contenedores "
          f"({rows['dev']['used_pct_of_cluster']:>5.1f} %) · prod: {rows['prod']['containers']:>2} contenedores "
          f"({rows['prod']['used_pct_of_cluster']:>5.1f} %) · prod pendientes: {rows['prod']['pending']}")


step("Elasticidad: un trabajo largo en dev (20 maps de 45 s) toma todo el clúster porque prod está vacía")
dev = sleep_job("dev", 20, 45)
for _ in range(6):
    time.sleep(4)
    snapshot("solo dev")

step("Llega un trabajo a prod (8 maps de 10 s): YARN le devuelve su 70 % quitándole contenedores a dev")
prod = sleep_job("prod", 8, 10)
for _ in range(10):
    time.sleep(3)
    snapshot("dev + prod")
prod.wait()
snapshot("prod terminó")
print("El primer 'prod pendientes: 1' es el ApplicationMaster de prod: también tuvo que esperar a que se liberara espacio.")
dev.wait()

step("Las dos aplicaciones: los contenedores de dev que mató la preemption")
apps = sorted(yarn_api("apps", states="FINISHED")["apps"]["app"], key=lambda a: a["startedTime"])[-2:]  # dev y prod
table([{
    "app": a["id"],
    "name": a["name"],
    "queue": a["queue"],
    "status": a["finalStatus"],
    "seconds": round(a["elapsedTime"] / 1000),
    "preempted_containers": a["numNonAMContainerPreempted"],
    "preempted_mb": a["preemptedResourceMB"],
} for a in apps])
print("Los maps de dev que murieron por preemption no cuentan como fallas: MapReduce los vuelve a lanzar cuando hay")
print("espacio, y el trabajo termina bien, solo más tarde. Sin preemption, prod habría esperado a que dev soltara solo.")
print("\nSiguiente: python lessons/07_mapreduce_internals.py")
