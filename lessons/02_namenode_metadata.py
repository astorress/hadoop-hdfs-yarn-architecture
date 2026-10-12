# Lección 02 - Los metadatos del NameNode: fsimage, edits, checkpoints y modo seguro.
#
# El NameNode guarda todo el árbol de archivos en memoria. Para no perderlo usa dos cosas en disco:
#   fsimage  una foto completa del árbol en una transacción dada
#   edits    el registro de cada cambio posterior (crear, renombrar, borrar...), uno por transacción (txid)
# Al arrancar, carga el último fsimage y reproduce los edits. Como los edits crecen sin parar, el standby hace
# checkpoints: aplica los edits a su fsimage, guarda uno nuevo y se lo envía al activo.
import os
import re
import tempfile
import time
from collections import Counter

from lab.config import RAW, WORK
from lab.hdfs import active_namenode, hdfs, jmx, namenode_states
from lab.show import step, table, title

NN1 = "/mnt/nn1"  # el volumen de datos de nn1, montado de solo lectura en el laboratorio
DEMO = f"{WORK}/02"

title("02 - Metadatos del NameNode")


def fsnamesystem(nn):
    return jmx(nn, "Hadoop:service=NameNode,name=FSNamesystem")


step("Los archivos de metadatos de nn1 en disco (/data/namenode/current)")
files = sorted(os.listdir(f"{NN1}/namenode/current"))
for name in files:
    print(f"  {name:55} {os.path.getsize(f'{NN1}/namenode/current/{name}'):>10,} bytes")
print("fsimage_N: foto del árbol hasta la transacción N · edits_A-B: transacciones A a B · edits_inprogress: el")
print("segmento abierto · seen_txid: la última transacción vista · VERSION: identidad del clúster")
print(open(f"{NN1}/namenode/current/VERSION").read().strip())

step("Cada operación sobre el árbol es una transacción con su número (txid)")
active = active_namenode()
hdfs("dfs", "-rm", "-r", "-f", "-skipTrash", DEMO)
hdfs("dfs", "-mkdir", "-p", f"{DEMO}/a")
hdfs("dfs", "-cp", f"{RAW}/meters/meters.csv", f"{DEMO}/a/meters.csv")
hdfs("dfs", "-mv", f"{DEMO}/a/meters.csv", f"{DEMO}/a/catalogo.csv")
hdfs("dfs", "-chmod", "640", f"{DEMO}/a/catalogo.csv")
hdfs("dfs", "-setrep", "2", f"{DEMO}/a/catalogo.csv")
hdfs("dfs", "-rm", "-r", "-skipTrash", f"{DEMO}/a")
after = next(r["last_txid"] for r in namenode_states() if r["namenode"] == active)
print(f"mkdir, cp, mv, chmod, setrep y rm hechos; el último txid del activo ({active}) es {after}")

step("rollEdits: cerrar el segmento de edits en curso; luego leerlo con el Offline Edits Viewer (hdfs oev)")
hdfs("dfsadmin", "-rollEdits")
time.sleep(1)
journal = f"{NN1}/journal/lake/current"  # los edits que guardó el JournalNode de nn1
segment = next(f for f in sorted(os.listdir(journal), reverse=True)
               if f.startswith("edits_0") and int(f.split("-")[1]) >= after)
with tempfile.TemporaryDirectory() as tmp:
    hdfs("oev", "-i", f"{journal}/{segment}", "-o", f"{tmp}/edits.xml")
    xml = open(f"{tmp}/edits.xml").read()
records = re.findall(r"<OPCODE>(\w+)</OPCODE>\s*<DATA>\s*<TXID>(\d+)</TXID>(.*?)</DATA>", xml, re.S)
rows = []
for opcode, txid, data in records:
    path = re.search(r"<(?:PATH|SRC)>([^<]+)", data)
    target = re.search(r"<DST>([^<]+)", data)
    if path and path.group(1).startswith(DEMO):  # solo las operaciones de esta lección
        rows.append({"txid": int(txid), "opcode": opcode,
                     "path": path.group(1) + (f" -> {target.group(1)}" if target else "")})
print(f"segmento {segment}:")
table(rows)
print("Un 'cp' son varias transacciones: OP_ADD abre el archivo, OP_ADD_BLOCK asigna cada bloque, OP_CLOSE lo cierra")
print("y OP_RENAME le quita el ._COPYING_. Entre ellas hay transacciones sin ruta (ids de bloque, generation stamps).")
print("Estos mismos registros viajan a los JournalNodes y el standby los aplica para tener el mismo árbol.")

step("El fsimage: descargarlo del activo (dfsadmin -fetchImage) y leerlo con el Offline Image Viewer (hdfs oiv)")
with tempfile.TemporaryDirectory() as tmp:
    hdfs("dfsadmin", "-fetchImage", tmp)
    image = next(f for f in os.listdir(tmp) if f.startswith("fsimage"))
    hdfs("oiv", "-p", "Delimited", "-i", f"{tmp}/{image}", "-o", f"{tmp}/fsimage.tsv")
    lines = open(f"{tmp}/fsimage.tsv").read().splitlines()
    header, entries = lines[0].split("\t"), [dict(zip(lines[0].split("\t"), line.split("\t"))) for line in lines[1:]]
    distribution = hdfs("oiv", "-p", "FileDistribution", "-i", f"{tmp}/{image}", "-o", "-")
print(f"{image}: {len(entries)} inodos (archivos y carpetas). Columnas: {', '.join(header[:8])}...")
table([{
    "path": e["Path"],
    "replication": e["Replication"],
    "blocks": e["BlocksCount"],
    "bytes": e["FileSize"],
    "permission": e["Permission"],
} for e in entries if e["Path"].startswith(f"{RAW}/readings")][:5])
print(Counter("carpeta" if e["Permission"].startswith("d") else "archivo" for e in entries))
print("\n".join(line for line in distribution.splitlines() if line.startswith(("totalFiles", "totalDirectories",
                                                                                 "totalBlocks", "totalSpace"))))

step("Checkpoint: el standby aplica los edits a su fsimage cada 2 minutos y se lo envía al activo")
table([{k: r[k] for k in ("namenode", "state", "last_txid", "checkpoint_txid")} for r in namenode_states()])
print("Lo que va de checkpoint_txid a last_txid son los edits que habría que reproducir si el NameNode se reiniciara.")
print("Sin checkpoints, un reinicio tendría que reproducir todos los edits desde el principio (y tardaría horas).")

step("Modo seguro: el NameNode solo lee. saveNamespace (fsimage nuevo a la fuerza) lo exige")
print(hdfs("dfsadmin", "-safemode", "enter"))
try:
    hdfs("dfs", "-mkdir", f"{DEMO}/en-modo-seguro")
except RuntimeError as error:
    print("mkdir en modo seguro ->", re.search(r"(SafeModeException|Cannot create directory)[^.]*", str(error)).group(0))
print("ls sí funciona:", len(hdfs("dfs", "-ls", RAW).splitlines()), "líneas")
print(hdfs("dfsadmin", "-saveNamespace"))
print(hdfs("dfsadmin", "-safemode", "leave"))
print("Al arrancar, el NameNode también está en modo seguro hasta que los DataNodes reportan suficientes bloques.")

step("Memoria del NameNode: todo el árbol vive en su heap")
memory = jmx(active, "java.lang:type=Memory")["HeapMemoryUsage"]
system = fsnamesystem(active)
objects = system["FilesTotal"] + system["BlocksTotal"]
print(f"heap usado: {memory['used'] / 2**20:.0f} MB de {memory['max'] / 2**20:.0f} MB · objetos: {objects:,}")
print("Regla práctica: ~1 GB de heap por cada millón de bloques. Por eso importan los archivos pequeños (lección 10).")
hdfs("dfs", "-rm", "-r", "-f", "-skipTrash", DEMO)
print("\nSiguiente: python lessons/03_ha_failover.py")
