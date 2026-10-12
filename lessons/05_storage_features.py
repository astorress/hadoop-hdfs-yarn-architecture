# Lección 05 - Funciones de almacenamiento de HDFS: cuotas, papelera, snapshots, permisos y ACLs, erasure coding.
#
# Además de guardar bloques, HDFS es un sistema de archivos multiusuario: limita cuánto puede crecer cada carpeta,
# guarda lo borrado un tiempo, toma fotos baratas de carpetas, controla quién lee qué y ofrece una alternativa a la
# replicación (erasure coding) que protege los datos con la mitad del espacio extra.
import re

from lab.config import RAW, WORK
from lab.hdfs import hdfs
from lab.show import step, table, title

DEMO = f"{WORK}/05"
DAY = f"{RAW}/readings/readings_2026-09-04.csv"  # 3,7 MB

title("05 - Funciones de almacenamiento")
hdfs("dfs", "-rm", "-r", "-f", "-skipTrash", DEMO)
hdfs("dfs", "-rm", "-r", "-f", "-skipTrash", "/user/hadoop/.Trash")
for path in ("snap", "acl", "quota", "ec", "trash"):
    hdfs("dfs", "-mkdir", "-p", f"{DEMO}/{path}")


def error_of(*args, user=None):
    try:
        hdfs(*args, user=user)
        return "OK"
    except RuntimeError as error:
        match = re.search(r"(\w+Exception): ([^\n]{0,150})", str(error))
        return f"{match.group(1)}: {match.group(2)}" if match else str(error).splitlines()[-1][:120]


step("Cuotas: máximo de nombres (archivos + carpetas) y de bytes en una carpeta")
quota = f"{DEMO}/quota"
hdfs("dfsadmin", "-setQuota", "3", quota)  # la carpeta misma cuenta como 1
print(hdfs("dfs", "-count", "-q", "-h", "-v", quota))
for i in (1, 2, 3):
    result = error_of("dfs", "-put", "/etc/hostname", f"{quota}/file{i}.txt")
    print(f"put file{i}.txt -> {result}")
print("-> 3 nombres: la carpeta y dos archivos. El tercero no cabe.")
hdfs("dfsadmin", "-clrQuota", quota)
hdfs("dfs", "-rm", "-skipTrash", f"{quota}/file*.txt")
hdfs("dfsadmin", "-setSpaceQuota", "30m", quota)
print("cuota de espacio de 30 MB, archivo de 3,7 MB con bloques de 128 MB:",
      error_of("dfs", "-cp", DAY, f"{quota}/day.csv"))
print("con bloques de 4 MB:", error_of("dfs", "-D", "dfs.blocksize=4m", "-cp", DAY, f"{quota}/day.csv"))
print(hdfs("dfs", "-count", "-q", "-h", "-v", quota))
print("Al pedir un bloque nuevo, HDFS reserva el bloque completo por cada réplica (128 MB x 3 = 384 MB) aunque el")
print("archivo sea pequeño; al cerrarlo solo cuenta lo real (3,7 MB x 3 = 11,1 MB).")

step("Papelera: rm mueve a /user/<usuario>/.Trash; se puede recuperar durante fs.trash.interval (60 min)")
hdfs("dfs", "-cp", DAY, f"{DEMO}/trash/day.csv")
hdfs("dfs", "-rm", f"{DEMO}/trash/day.csv")
trashed = hdfs("dfs", "-ls", "-R", "/user/hadoop/.Trash").splitlines()[-1].split()[-1]
print("en la papelera:", trashed)
hdfs("dfs", "-mv", trashed, f"{DEMO}/trash/day.csv")
print("recuperado:    ", hdfs("dfs", "-ls", "-C", f"{DEMO}/trash"))
print("-skipTrash borra de verdad; hdfs dfs -expunge vacía la papelera antes de tiempo.")

step("Snapshots: fotos de solo lectura de una carpeta, que no copian datos")
snap = f"{DEMO}/snap"
for day in ("05", "06"):
    hdfs("dfs", "-cp", f"{RAW}/readings/readings_2026-09-{day}.csv", f"{snap}/day{day}.csv")
hdfs("dfsadmin", "-allowSnapshot", snap)
hdfs("dfs", "-createSnapshot", snap, "s1")
hdfs("dfs", "-rm", "-skipTrash", f"{snap}/day05.csv")
hdfs("dfs", "-mv", f"{snap}/day06.csv", f"{snap}/september-06.csv")
hdfs("dfs", "-cp", f"{RAW}/readings/readings_2026-09-07.csv", f"{snap}/day07.csv")
hdfs("dfs", "-createSnapshot", snap, "s2")
print(hdfs("snapshotDiff", snap, "s1", "s2"))
print("hoy:        ", hdfs("dfs", "-ls", "-C", snap))
print("en s1:      ", hdfs("dfs", "-ls", "-C", f"{snap}/.snapshot/s1"))
hdfs("dfs", "-cp", f"{snap}/.snapshot/s1/day05.csv", snap)
print("restaurado: ", hdfs("dfs", "-ls", "-C", snap))
print(hdfs("dfs", "-du", "-s", "-h", "-v", snap))
print("El snapshot es una entrada en el árbol del NameNode: los bloques del archivo borrado siguen vivos porque s1 los")
print("referencia. Cuesta espacio solo lo que cambia después.")
for name in ("s1", "s2"):
    hdfs("dfs", "-deleteSnapshot", snap, name)

step("Permisos POSIX y ACLs: sin Kerberos, HDFS confía en el usuario que dice el cliente (HADOOP_USER_NAME)")
acl = f"{DEMO}/acl"
hdfs("dfs", "-cp", f"{RAW}/inspections/inspections.csv", f"{acl}/inspections.csv")
hdfs("dfs", "-chmod", "-R", "750", acl)
print(hdfs("dfs", "-ls", "-d", acl))
print("ana lee la carpeta:          ", error_of("dfs", "-ls", acl, user="ana"))
hdfs("dfs", "-setfacl", "-m", "user:ana:r-x", acl)
hdfs("dfs", "-setfacl", "-m", "user:ana:r--", f"{acl}/inspections.csv")
print("con ACL user:ana:r-x:        ", error_of("dfs", "-ls", acl, user="ana"))
print("ana lee el archivo:          ", error_of("dfs", "-head", f"{acl}/inspections.csv", user="ana"))
print("ana intenta borrarlo:        ", error_of("dfs", "-rm", "-skipTrash", f"{acl}/inspections.csv", user="ana"))
print("luis (sin ACL) lee la carpeta:", error_of("dfs", "-ls", acl, user="luis"))
print(hdfs("dfs", "-ls", "-d", acl))
print(hdfs("dfs", "-getfacl", acl))
print("El '+' al final de los permisos en 'hdfs dfs -ls' indica que hay ACLs. En producción la identidad la da")
print("Kerberos: aquí cualquiera puede decir que es 'hadoop' (el superusuario) y saltarse todo.")

step("Erasure coding: en lugar de 3 copias, 2 bloques de datos + 1 de paridad (política XOR-2-1-1024k)")
print(hdfs("ec", "-enablePolicy", "-policy", "XOR-2-1-1024k"))
ec = f"{DEMO}/ec"
hdfs("ec", "-setPolicy", "-path", ec, "-policy", "XOR-2-1-1024k")
hdfs("dfs", "-cp", DAY, f"{ec}/day.csv")
hdfs("dfs", "-cp", DAY, f"{DEMO}/day_replicated.csv")
print(hdfs("ec", "-getPolicy", "-path", ec))
print(hdfs("dfs", "-du", "-h", "-v", f"{ec}/day.csv", f"{DEMO}/day_replicated.csv"))
fsck = hdfs("fsck", f"{ec}/day.csv", "-files", "-blocks", "-locations")
groups = re.findall(r"blk_-\d+_\d+ len=(\d+) Live_repl=(\d+)", fsck)
print(f"grupos de bloques: {len(groups)} · cada uno repartido en 3 DataNodes (2 de datos + 1 de paridad)")
table([{"group": i, "bytes": int(length), "internal_blocks": int(repl)} for i, (length, repl) in enumerate(groups)])
print("Espacio: 1,5x en lugar de 3x. Tolera perder 1 DataNode (con 3 copias se pueden perder 2). Leer un archivo")
print("con un bloque perdido obliga a reconstruirlo con la paridad (CPU y red); por eso se usa para datos fríos.")

step("Checksums: el mismo contenido da el mismo checksum aunque esté guardado distinto (COMPOSITE_CRC)")
rows = []
for path in (f"{ec}/day.csv", f"{DEMO}/day_replicated.csv"):
    line = hdfs("dfs", "-D", "dfs.checksum.combine.mode=COMPOSITE_CRC", "-checksum", path)
    rows.append({"file": path, "checksum": line.split()[-1]})
table(rows)
hdfs("dfs", "-rm", "-r", "-skipTrash", DEMO)
print("\nSiguiente: python lessons/06_yarn_scheduler.py")
