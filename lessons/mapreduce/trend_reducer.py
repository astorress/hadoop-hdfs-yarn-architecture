# Reducer de la lección 07: recibe las lecturas de cada medidor juntas (partición por medidor) y ordenadas por fecha
# (orden secundario). Con eso compara la primera semana con la última: una caída fuerte sugiere fraude.
import sys


def flush(meter_id, days):
    totals = [kwh for _, kwh in sorted(days.items())]
    first, last = sum(totals[:7]) / 7, sum(totals[-7:]) / 7
    print(f"{meter_id}\t{len(totals)}\t{sum(totals):.1f}\t{first:.2f}\t{last:.2f}\t{last / first:.2f}")


current, days, order_ok = None, {}, True
previous_day = ""
for line in sys.stdin:
    meter_id, day, kwh = line.rstrip("\n").split("\t")
    if meter_id != current:
        if current:
            flush(current, days)
        current, days, previous_day = meter_id, {}, ""
    if day < previous_day:
        print("reporter:counter:Orden,Fechas fuera de orden,1", file=sys.stderr)
    previous_day = day
    days[day] = days.get(day, 0.0) + float(kwh)
if current:
    flush(current, days)
