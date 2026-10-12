# Mapper que solo ocupa su contenedor: consume su línea de entrada y espera N segundos (lección 06).
import sys
import time

sys.stdin.read()
time.sleep(float(sys.argv[1]))
