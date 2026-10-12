# Reduce-side join (lección 08): el mapper lee dos tipos de archivo y etiqueta cada registro con su origen.
# Clave compuesta meter_id + etiqueta: "0" para el catálogo y "1" para las lecturas. Así, con orden secundario, el
# reducer recibe primero la fila del catálogo de cada medidor y después sus lecturas.
import sys

for line in sys.stdin:
    fields = line.rstrip("\n").split(",")
    if fields[0] == "meter_id":
        continue
    if len(fields) == 7:  # meters.csv: meter_id, customer_type, stratum, city, feeder, contracted_kw, installed_at
        print(f"{fields[0]}\t0\t{fields[3]}\t{fields[1]}")
    else:  # readings: meter_id, ts, kwh, voltage
        print(f"{fields[0]}\t1\t{abs(float(fields[2]))}")
