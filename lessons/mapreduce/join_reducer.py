# Reduce-side join (lección 08): por cada medidor llega primero su fila del catálogo (etiqueta 0) y luego sus
# lecturas (etiqueta 1). Emite ciudad, tipo de cliente y energía total del medidor.
import sys

meter, city, kind, total = None, None, None, 0.0


def flush():
    if meter and city:
        print(f"{city}\t{kind}\t{total:.3f}")


for line in sys.stdin:
    fields = line.rstrip("\n").split("\t")
    if fields[0] != meter:
        flush()
        meter, city, kind, total = fields[0], None, None, 0.0
    if fields[1] == "0":
        city, kind = fields[2], fields[3]
    else:
        total += float(fields[2])
flush()
