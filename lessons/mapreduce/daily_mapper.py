# Mapper de la lección 07: una línea por lectura -> "meter_id<TAB>fecha<TAB>kWh".
# La clave son los dos primeros campos (stream.num.map.output.key.fields=2). Además cuenta problemas de calidad con
# contadores de Hadoop: una línea "reporter:counter:<grupo>,<contador>,<n>" por stderr.
import sys


def counter(name):
    print(f"reporter:counter:Calidad,{name},1", file=sys.stderr)


for line in sys.stdin:
    meter_id, ts, kwh, voltage = line.rstrip("\n").split(",")
    if meter_id == "meter_id":
        continue
    kwh = float(kwh)
    if kwh < 0:
        counter("kWh negativo")
        kwh = -kwh
    if voltage == "":
        counter("Voltaje vacío")
    if kwh == 0:
        counter("kWh en cero")
    print(f"{meter_id}\t{ts[:10]}\t{kwh}")
