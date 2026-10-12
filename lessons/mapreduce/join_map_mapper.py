# Map-side join (lección 08): el catálogo (pequeño) llega a cada map por la caché distribuida (-files) y se carga en
# memoria. Cada lectura se enriquece en el map: no hace falta llevar el catálogo por el shuffle.
import csv
import sys

catalog = {row["meter_id"]: (row["city"], row["customer_type"]) for row in csv.DictReader(open("meters.csv"))}

for line in sys.stdin:
    meter_id, ts, kwh, voltage = line.rstrip("\n").split(",")
    if meter_id == "meter_id":
        continue
    city, kind = catalog[meter_id]
    print(f"{city}\t{kind}\t{abs(float(kwh))}")
