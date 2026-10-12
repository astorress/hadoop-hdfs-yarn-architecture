# Lección 07 - MapReduce por dentro: claves compuestas, particionador, orden secundario, combiner y contadores.
#
# El trabajo calcula, para cada medidor, la energía de cada día y compara la primera semana con la última. Para eso
# cada reducer necesita (1) todas las lecturas de un medidor y (2) en orden de fecha. Se logra con:
#   clave compuesta    meter_id + fecha (los dos primeros campos de la salida del map)
#   particionador      reparte por meter_id solamente: todas las fechas de un medidor van al mismo reducer
#   comparador         ordena por meter_id y luego por fecha: el reducer las recibe en orden (orden secundario)
from lab import dataset
from lab.config import RAW, WORK
from lab.hdfs import hdfs, history_api
from lab.mapreduce import streaming
from lab.show import step, table, title

OUT = f"{WORK}/07/trend"
SCRIPTS = [f"lessons/mapreduce/{s}.py" for s in ("daily_mapper", "daily_combiner", "trend_reducer")]

title("07 - MapReduce por dentro")

step("El trabajo: 30 archivos -> map (medidor, fecha, kWh) -> combiner -> 4 reducers particionados por medidor")
job = streaming(
    "meter-trend",
    input=f"{RAW}/readings",
    output=OUT,
    mapper="python3 daily_mapper.py",
    reducer="python3 trend_reducer.py",
    files=SCRIPTS,
    generic=[
        "-D", "mapreduce.job.reduces=4",
        "-D", "stream.num.map.output.key.fields=2",  # clave = meter_id + fecha
        "-D", "stream.num.reduce.output.key.fields=2",  # también a la salida del combiner (es un reducer)
        "-D", "mapreduce.partition.keypartitioner.options=-k1,1",  # particionar solo por meter_id
        "-D", "mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator",
        "-D", "mapreduce.partition.keycomparator.options=-k1,1 -k2,2",  # ordenar por meter_id y fecha
    ],
    options=["-combiner", "python3 daily_combiner.py",
             "-partitioner", "org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner"],
)
print(f"{job['job']} terminó en {job['seconds']} s")

step("Contadores del framework: el recorrido de los datos de map a reduce")
c = job["counters"]
table([{"counter": name, "value": c.get(name, 0)} for name in (
    "Launched map tasks", "Data-local map tasks", "Rack-local map tasks", "Launched reduce tasks",
    "Map input records", "Map output records", "Combine input records", "Combine output records",
    "Reduce shuffle bytes", "Reduce input groups", "Reduce input records", "Reduce output records",
    "Spilled Records", "GC time elapsed (ms)", "CPU time spent (ms)")])
print("Combine: de 2,87 millones de lecturas a ~30.000 sumas (medidor, día) antes de cruzar la red.")
print("Reduce input groups: una por clave compuesta (medidor, día). Spilled Records: lo que se escribió a disco local")
print("al ordenar (el buffer de orden es de mapreduce.task.io.sort.mb = 64 MB).")

step("Contadores propios (escritos por el mapper en stderr con 'reporter:counter:')")
table([{"counter": name, "value": c.get(name, 0)} for name in ("kWh negativo", "Voltaje vacío", "kWh en cero",
                                                              "Fechas fuera de orden")])
print("'Fechas fuera de orden' en 0 confirma el orden secundario: cada reducer recibió las fechas ordenadas.")
print("kWh en cero: las lecturas del corte de 3 horas del circuito BAQ-03 el 17 de septiembre.")

step("Un archivo por reducer: el particionador decidió qué medidores le tocaron a cada uno")
rows = []
for part in hdfs("dfs", "-ls", "-C", OUT).splitlines():
    if "part-" in part:
        lines = hdfs("dfs", "-cat", part).splitlines()
        rows.append({"file": part.split("/")[-1], "meters": len(lines), "first": lines[0].split("\t")[0],
                     "last": lines[-1].split("\t")[0]})
table(rows)
print("El particionador usa hash(meter_id) % 4: cada medidor cae siempre en el mismo reducer, y el reparto es parejo.")

step("El resultado: medidores cuya última semana cayó más frente a la primera")
results = [line.split("\t") for line in hdfs("dfs", "-cat", f"{OUT}/part-*").splitlines()]
ranking = sorted(results, key=lambda r: float(r[5]))[:8]
table([{"meter_id": r[0], "days": int(r[1]), "kwh_month": float(r[2]), "first_week": float(r[3]),
        "last_week": float(r[4]), "ratio": float(r[5]), "really_fraud": r[0] in dataset.FRAUD} for r in ranking])

step("Las tareas del trabajo según JobHistory: dónde corrió cada una y cuánto tardó")
tasks = history_api(f"mapreduce/jobs/{job['job']}/tasks")["tasks"]["task"]
attempts = []
for task in tasks:
    attempt = history_api(f"mapreduce/jobs/{job['job']}/tasks/{task['id']}/attempts")["taskAttempts"]["taskAttempt"]
    attempt = attempt[0] if isinstance(attempt, list) else attempt
    attempts.append({"type": task["type"], "node": attempt["nodeHttpAddress"].split(":")[0],
                     "seconds": round(task["elapsedTime"] / 1000, 1)})
summary = {}
for a in attempts:
    key = (a["type"], a["node"])
    summary.setdefault(key, []).append(a["seconds"])
table([{"type": t, "node": n, "tasks": len(s), "avg_seconds": round(sum(s) / len(s), 1)}
       for (t, n), s in sorted(summary.items())])
print("\nSiguiente: python lessons/08_mapreduce_joins.py")
