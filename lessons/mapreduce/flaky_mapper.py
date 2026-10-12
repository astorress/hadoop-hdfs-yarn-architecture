# Mapper de la lección 09 que falla a propósito, según el modo que recibe:
#   first    los maps de los días 01-05 fallan en su primer intento y funcionan al reintentar
#   always   el map del día 13 falla siempre
#   slow     el map del día 20 tarda 60 s (los demás, nada): un candidato a ejecución especulativa
# Hadoop Streaming pasa la configuración del trabajo como variables de entorno (puntos -> guiones bajos).
import os
import sys
import time

mode = sys.argv[1]
attempt = os.environ["mapreduce_task_attempt_id"]  # attempt_<job>_m_000003_0: el último número es el intento
source = os.environ.get("mapreduce_map_input_file", "")
day = source.rsplit("_", 1)[-1][8:10] if "readings_" in source else ""

if mode == "first" and day in ("01", "02", "03", "04", "05") and attempt.endswith("_0"):
    sys.exit(f"falla simulada en el intento {attempt} ({source.rsplit('/', 1)[-1]})")
if mode == "always" and day == "13":
    raise RuntimeError(f"registro corrupto en {source.rsplit('/', 1)[-1]}")
if mode == "slow" and day == "20":
    time.sleep(60)

rows = 0
for line in sys.stdin:
    rows += 1
print(f"{day}\t{rows}")
