import os

# Direcciones del clúster (.envs/lab.env). HDFS y YARN tienen dos nodos maestros cada uno: hay que preguntar cuál
# está activo. La CLI y los clientes Java lo resuelven solos con la configuración de /hadoop/conf.
NAMENODES = os.getenv("NAMENODES", "nn1,nn2").split(",")
NAMENODE_HTTP_PORT = int(os.getenv("NAMENODE_HTTP_PORT", "9870"))
RESOURCEMANAGERS = os.getenv("RESOURCEMANAGERS", "nn1,nn2").split(",")
JOBHISTORY_HTTP = os.getenv("JOBHISTORY_HTTP", "http://nn2:19888")
ZOOKEEPER = os.getenv("ZOOKEEPER", "zk1:2181,zk2:2181,zk3:2181")
WORKERS = ["worker1", "worker2", "worker3"]

# Datos en HDFS
RAW = "/data/raw"  # los CSV tal como llegan del sistema de medición
WORK = "/data/work"  # salidas de las lecciones
EXCLUDE_FILE = "/hadoop-admin/dfs.exclude"  # DataNodes en decommission (lección 04)
