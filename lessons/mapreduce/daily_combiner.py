# Combiner de la lección 07: suma los kWh de cada (medidor, día) dentro de un map, antes del shuffle.
import sys

current, total = None, 0.0
for line in sys.stdin:
    meter_id, day, kwh = line.rstrip("\n").split("\t")
    if (meter_id, day) != current:
        if current:
            print(f"{current[0]}\t{current[1]}\t{total:.3f}")
        current, total = (meter_id, day), 0.0
    total += float(kwh)
if current:
    print(f"{current[0]}\t{current[1]}\t{total:.3f}")
