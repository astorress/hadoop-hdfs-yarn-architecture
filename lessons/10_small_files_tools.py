# Lección 10 - Archivos pequeños y herramientas de datos: HAR, compactación y DistCp.
#
# HDFS está hecho para pocos archivos grandes. Cada archivo y cada bloque es un objeto en la memoria del NameNode, y
# cada archivo es al menos una tarea de map. Mil archivos de 4 KB cuestan al NameNode lo mismo que mil de 1 GB.
# Hadoop trae herramientas (que son trabajos de MapReduce) para agrupar archivos y para copiar datos en paralelo.
import os
import shutil
import tempfile
from collections import defaultdict

from lab.config import RAW, WORK
from lab.hdfs import active_namenode, hdfs, jmx, run
from lab.mapreduce import submit
from lab.show import step, table, title

DEMO = f"{WORK}/10"
DAY = f"{RAW}/readings/readings_2026-09-08.csv"

title("10 - Archivos pequeños y herramientas")
hdfs("dfs", "-rm", "-r", "-f", "-skipTrash", DEMO)
hdfs("dfs", "-mkdir", "-p", DEMO)


def namenode_objects():
    system = jmx(active_namenode(), "Hadoop:service=NameNode,name=FSNamesystem")
    return {"files_and_dirs": system["FilesTotal"], "blocks": system["BlocksTotal"]}


step("Un día de lecturas partido en un archivo por medidor (como lo dejaría un sistema que escribe por equipo)")
local = tempfile.mkdtemp()
rows = defaultdict(list)
for line in hdfs("dfs", "-cat", DAY).splitlines()[1:]:
    rows[line.split(",")[0]].append(line)
os.makedirs(f"{local}/small")
for meter_id, lines in rows.items():
    with open(f"{local}/small/{meter_id}.csv", "w") as file:
        file.write("\n".join(lines) + "\n")
before = namenode_objects()
hdfs("dfs", "-put", f"{local}/small", f"{DEMO}/small")
after = namenode_objects()
shutil.rmtree(local)
print(hdfs("dfs", "-count", "-h", "-v", f"{DEMO}/small"))
table([{"": "antes", **before}, {"": "después", **after},
       {"": "diferencia", **{k: after[k] - before[k] for k in after}}])
print("3,7 MB de datos agregaron ~2.000 objetos al NameNode (1.000 archivos + 1.000 bloques). El mismo día en un solo")
print("archivo es 1 archivo y 1 bloque. Y un trabajo de MapReduce sobre esta carpeta lanzaría 1.000 maps.")

step("HAR (Hadoop Archive): empaquetar la carpeta en un archivo que se sigue leyendo como carpeta")
har = submit(["hadoop", "archive", "-archiveName", "day08.har", "-p", f"{DEMO}/small", f"{DEMO}/archive"])
print(f"hadoop archive (un trabajo de MapReduce) terminó en {har['seconds']} s")
print(hdfs("dfs", "-count", "-h", "-v", f"{DEMO}/archive/day08.har"))
print(hdfs("dfs", "-ls", "-C", f"{DEMO}/archive/day08.har"))
files = hdfs("dfs", "-ls", "-C", f"har://{DEMO}/archive/day08.har").splitlines()
print(f"vistos por har://: {len(files)} archivos, p. ej. {files[0]}")
print(hdfs("dfs", "-cat", f"har://{DEMO}/archive/day08.har/M00001.csv").splitlines()[:2])
hdfs("dfs", "-rm", "-r", "-skipTrash", f"{DEMO}/small")
print("Tras borrar la carpeta original:", namenode_objects())
print("_index y _masterindex dicen dónde empieza cada archivo dentro de part-0. Los HAR son de solo lectura: para")
print("datos que siguen llegando se compacta (siguiente paso) o se usa un formato que agrupa filas (Parquet, ORC).")

step("Compactación: unir los archivos pequeños en uno grande (getmerge) y volver a subirlo")
hdfs("dfs", "-mkdir", "-p", f"{DEMO}/compact")
hdfs("dfs", "-getmerge", f"har://{DEMO}/archive/day08.har", "/tmp/day08.csv")
hdfs("dfs", "-put", "-f", "/tmp/day08.csv", f"{DEMO}/compact/day08.csv")
os.remove("/tmp/day08.csv")
print(hdfs("dfs", "-count", "-h", "-v", f"{DEMO}/compact"))
print("getmerge pasa todo por el cliente: sirve para un día, no para un año. A escala se compacta con un trabajo")
print("distribuido (MapReduce o Spark) que lee muchos archivos pequeños y escribe pocos grandes.")

step("DistCp: copiar /data/raw a otra ruta con un trabajo de MapReduce (cada map copia un grupo de archivos)")
backup = f"{DEMO}/backup"
copy = submit(["hadoop", "distcp", "-m", "4", RAW, backup])
c = copy["counters"]
table([{"run": "completa", "seconds": copy["seconds"], "maps": c.get("Launched map tasks"),
        "files_copied": c.get("Files Copied", 0), "files_skipped": c.get("Files Skipped", 0),
        "mb_copied": round(c.get("Bytes Copied", 0) / 2**20, 1)}])

step("DistCp -update: solo copia lo que cambió (compara tamaño y checksum)")
hdfs("dfs", "-cp", f"{DEMO}/compact/day08.csv", f"{RAW}/readings/extra_day08.csv")
update = submit(["hadoop", "distcp", "-update", "-m", "4", f"{RAW}/readings", f"{backup}/readings"])
c = update["counters"]
table([{"run": "-update", "seconds": update["seconds"], "maps": c.get("Launched map tasks"),
        "files_copied": c.get("Files Copied", 0), "files_skipped": c.get("Files Skipped", 0),
        "mb_copied": round(c.get("Bytes Copied", 0) / 2**20, 1)}])
hdfs("dfs", "-rm", "-skipTrash", f"{RAW}/readings/extra_day08.csv")
print("Así se respalda un clúster en otro (hdfs://clúster-a/... hdfs://clúster-b/...) o se migra a S3 (s3a://):")
print("la primera vez se copia todo y luego solo las diferencias.")

step("Lo que quedó en HDFS")
print(run("hdfs", "dfs", "-du", "-h", DEMO))
print("\nFin de las lecciones. El sandbox (notebooks/sandbox.ipynb) junta todo para experimentar.")
