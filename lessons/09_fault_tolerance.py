# Lección 09 - Tolerancia a fallos en MapReduce y YARN: reintentos, fallas permanentes, porcentaje de fallas
# tolerado, logs agregados y ejecución especulativa.
#
# Una tarea que falla no tumba el trabajo: el ApplicationMaster la vuelve a lanzar (hasta mapreduce.map.maxattempts
# veces, en otro nodo si puede). Como cada map lee un bloque inmutable de HDFS y escribe su salida aparte, rehacerlo
# da el mismo resultado. Solo si una tarea agota sus intentos falla el trabajo completo.
import re
import time

from lab.config import RAW, WORK
from lab.hdfs import history_api, run
from lab.mapreduce import streaming
from lab.show import step, table, title

OUT = f"{WORK}/09"
MAPPER = "lessons/mapreduce/flaky_mapper.py"

title("09 - Tolerancia a fallos")


def job(mode, *generic, check=True):
    return streaming(f"flaky-{mode}", input=f"{RAW}/readings", output=f"{OUT}/{mode}",
                     mapper=f"python3 flaky_mapper.py {mode}", files=[MAPPER],
                     generic=["-D", "mapreduce.map.maxattempts=3", *generic], check=check)


def attempts(job_id, only_failed=True):
    rows = []
    for task in history_api(f"mapreduce/jobs/{job_id}/tasks")["tasks"]["task"]:
        found = history_api(f"mapreduce/jobs/{job_id}/tasks/{task['id']}/attempts")["taskAttempts"]["taskAttempt"]
        found = found if isinstance(found, list) else [found]
        if only_failed and len(found) == 1 and found[0]["state"] == "SUCCEEDED":
            continue
        for a in sorted(found, key=lambda a: a["id"]):
            rows.append({"attempt": a["id"].split("_m_")[-1], "state": a["state"], "node": a["nodeHttpAddress"].split(":")[0],
                         "seconds": round(a["elapsedTime"] / 1000, 1), "diagnostics": a.get("diagnostics", "")[:60]})
    return rows


step("1. Fallas transitorias: 5 maps fallan en su primer intento y se recuperan solos")
first = job("first")
print(f"{first['job']}: {'SUCCEEDED' if first['ok'] else 'FAILED'} en {first['seconds']} s · maps lanzados: "
      f"{first['counters']['Launched map tasks']} · fallidos: {first['counters'].get('Failed map tasks', 0)}")
table(attempts(first["job"]))
print("Intento _0 FAILED y _1 SUCCEEDED en cada uno: 30 maps, 35 lanzamientos. El trabajo ni se enteró.")

step("2. Una falla permanente: el map del día 13 falla en sus 3 intentos y el trabajo entero falla")
always = job("always", check=False)
history = history_api(f"mapreduce/jobs/{always['job']}")["job"]
print(f"{always['job']}: {history['state']} · {history['diagnostics'][:160]}")
table(attempts(always["job"])[:3])

step("3. El error real está en los logs del contenedor, agregados en HDFS al terminar (yarn logs)")
app = always["job"].replace("job_", "application_")
for _ in range(30):  # los NodeManagers suben los logs a HDFS unos segundos después de que termina la aplicación
    logs = run("yarn", "logs", "-applicationId", app, "-log_files", "stderr", check=False)
    if "RuntimeError" in logs:
        break
    time.sleep(2)
blocks = re.findall(r"Container: (\S+) on (\S+)\n.*?LogType:stderr.*?(RuntimeError: [^\n]+)", logs, re.S)
table([{"container": c.split("_")[-1], "node": n.split("_")[0], "error": e} for c, n, e in blocks])
print("Cada intento corrió en su propio contenedor; el traceback de Python quedó en su stderr. Los logs se pueden")
print("leer después de que la aplicación terminó porque yarn.log-aggregation-enable los copia a HDFS (/tmp/logs).")

step("4. Tolerar un porcentaje de maps fallidos (mapreduce.map.failures.maxpercent)")
tolerant = job("always", "-D", "mapreduce.map.failures.maxpercent=5", check=False)
print(f"{tolerant['job']}: {'SUCCEEDED' if tolerant['ok'] else 'FAILED'} · intentos de map fallidos: "
      f"{tolerant['counters'].get('Failed map tasks', 0)} (los 3 del map del día 13, que se dio por perdido)")
print("Con hasta un 5 % de maps fallidos permitido, el trabajo termina sin los datos del día 13. Útil cuando unos")
print("pocos archivos dañados no deben frenar un proceso nocturno, pero hay que vigilar ese contador.")

step("5. Ejecución especulativa: un map lento (60 s contra ~5 s de los demás) recibe un intento de respaldo")
slow = job("slow", "-D", "mapreduce.map.speculative=true",
           "-D", "mapreduce.job.speculative.minimum-allowed-tasks=2",
           "-D", "mapreduce.job.speculative.retry-after-speculate=5000")
print(f"{slow['job']}: {slow['seconds']} s · maps lanzados: {slow['counters']['Launched map tasks']}")
table(attempts(slow["job"]))
print("El ApplicationMaster vio un map mucho más lento que el resto y lanzó una copia (intento _1) en otro nodo.")
print("Gana el que termine primero y el otro se mata (KILLED). Si la lentitud viene de un nodo con problemas, la copia")
print("gana; si viene de los datos (este map tiene más trabajo que los demás), el original gana y la copia fue en vano:")
print("la especulación ayuda con nodos lentos, no con datos desbalanceados (skew).")
print("\nSiguiente: python lessons/10_small_files_tools.py")
