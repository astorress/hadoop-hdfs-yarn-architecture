# Suma el último campo agrupando por los anteriores (clave de 2 campos). Sirve de combiner y de reducer (lección 08).
import sys

current, total = None, 0.0
for line in sys.stdin:
    *key, value = line.rstrip("\n").split("\t")
    key = tuple(key)
    if key != current:
        if current:
            print("\t".join(current) + f"\t{total:.3f}")
        current, total = key, 0.0
    total += float(value)
if current:
    print("\t".join(current) + f"\t{total:.3f}")
