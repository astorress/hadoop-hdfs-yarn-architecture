# Lección 08 - Joins en MapReduce: energía por ciudad y tipo de cliente uniendo lecturas con el catálogo.
#
# Las lecturas solo traen meter_id; la ciudad y el tipo de cliente están en meters.csv. Hay dos formas de unirlas:
#   reduce-side join  las dos fuentes van al shuffle con la misma clave (meter_id) y el reducer las junta.
#                     Funciona con tablas de cualquier tamaño, pero baraja todo por la red.
#   map-side join     la tabla pequeña viaja a cada map por la caché distribuida y la unión se hace en el map.
#                     Casi no hay shuffle, pero la tabla pequeña debe caber en la memoria de cada map.
# Es lo mismo que en Spark son el sort-merge join y el broadcast join.
from collections import defaultdict

from lab.config import RAW, WORK
from lab.hdfs import hdfs
from lab.mapreduce import streaming
from lab.show import step, table, title

OUT = f"{WORK}/08"
SCRIPTS = "lessons/mapreduce"

title("08 - Joins en MapReduce")


def summary(job):
    c = job["counters"]
    return {"seconds": job["seconds"], "maps": c["Launched map tasks"], "map_output": c["Map output records"],
            "combine_output": c.get("Combine output records", 0), "shuffle_mb": round(c["Reduce shuffle bytes"] / 2**20, 2)}


step("Reduce-side join: catálogo y lecturas al shuffle con clave (meter_id, etiqueta)")
reduce_side = streaming(
    "join-reduce-side",
    input=f"{RAW}/readings,{RAW}/meters",
    output=f"{OUT}/reduce_side",
    mapper="python3 join_reduce_mapper.py",
    reducer="python3 join_reducer.py",
    files=[f"{SCRIPTS}/join_reduce_mapper.py", f"{SCRIPTS}/join_reducer.py"],
    generic=["-D", "mapreduce.job.reduces=4",
             "-D", "stream.num.map.output.key.fields=2",
             "-D", "mapreduce.partition.keypartitioner.options=-k1,1",
             "-D", "mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator",
             "-D", "mapreduce.partition.keycomparator.options=-k1,1 -k2,2n"],
    options=["-partitioner", "org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner"],
)
table([summary(reduce_side)])
print("Cada lectura viaja por el shuffle (2,87 M registros) solo para encontrarse con su fila del catálogo.")

step("Map-side join: meters.csv viaja a cada map por la caché distribuida (-files)")
hdfs("dfs", "-get", "-f", f"{RAW}/meters/meters.csv", "/tmp/meters.csv")
map_side = streaming(
    "join-map-side",
    input=f"{RAW}/readings",
    output=f"{OUT}/map_side",
    mapper="python3 join_map_mapper.py",
    reducer="python3 sum_reducer.py",
    files=[f"{SCRIPTS}/join_map_mapper.py", f"{SCRIPTS}/sum_reducer.py", "/tmp/meters.csv"],
    generic=["-D", "mapreduce.job.reduces=1", "-D", "stream.num.map.output.key.fields=2",
             "-D", "stream.num.reduce.output.key.fields=2"],
    options=["-combiner", "python3 sum_reducer.py"],
)
table([summary(map_side)])
print("Con el catálogo en memoria, cada map ya emite (ciudad, tipo) y el combiner las suma: al shuffle llegan unas")
print("pocas decenas de registros por map.")

step("Los dos caminos llegan al mismo resultado")
by_reduce = defaultdict(float)
for line in hdfs("dfs", "-cat", f"{OUT}/reduce_side/part-*").splitlines():
    city, kind, kwh = line.split("\t")
    by_reduce[(city, kind)] += float(kwh)
by_map = {}
for line in hdfs("dfs", "-cat", f"{OUT}/map_side/part-*").splitlines():
    city, kind, kwh = line.split("\t")
    by_map[(city, kind)] = float(kwh)
table([{"city": city, "customer_type": kind, "mwh_reduce_side": round(by_reduce[(city, kind)] / 1000, 2),
        "mwh_map_side": round(kwh / 1000, 2)} for (city, kind), kwh in sorted(by_map.items(), key=lambda x: -x[1])][:8])
print("El reduce-side join necesitó una segunda pasada (aquí en Python) para sumar por ciudad y tipo; el map-side")
print("lo hizo en el mismo trabajo. En MapReduce cada paso extra es otro trabajo que escribe a HDFS: Spark encadena")
print("todo en memoria, que es por lo que reemplazó a MapReduce en casi todos los usos.")
print("\nSiguiente: python lessons/09_fault_tolerance.py")
