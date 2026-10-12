import json
import os
import re
import subprocess

import requests

from lab.config import JOBHISTORY_HTTP, NAMENODE_HTTP_PORT, NAMENODES, RESOURCEMANAGERS


# La CLI de Hadoop (hdfs, yarn, mapred) lee la configuración del clúster de HADOOP_CONF_DIR=/hadoop/conf
def run(*command, check=True, env=None, input=None):
    result = subprocess.run(command, capture_output=True, text=True, env=env and {**os.environ, **env}, input=input)
    if check and result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result.stdout.strip()


def hdfs(*args, check=True, user=None, input=None):
    """hdfs <args>. user= ejecuta como otro usuario (sin Kerberos, HDFS confía en HADOOP_USER_NAME)."""
    return run("hdfs", *args, check=check, env={"HADOOP_USER_NAME": user} if user else None, input=input)


# JMX: cada demonio de Hadoop publica sus métricas internas como JSON en su puerto web
def jmx(host, bean, port=NAMENODE_HTTP_PORT):
    beans = requests.get(f"http://{host}:{port}/jmx", params={"qry": bean}, timeout=5).json()["beans"]
    return beans[0] if beans else {}


def namenode_states():
    rows = []
    for nn in NAMENODES:
        try:
            status = jmx(nn, "Hadoop:service=NameNode,name=NameNodeStatus")
            system = jmx(nn, "Hadoop:service=NameNode,name=FSNamesystem")
            info = jmx(nn, "Hadoop:service=NameNode,name=NameNodeInfo")
            journal = json.loads(info["JournalTransactionInfo"])
            rows.append({
                "namenode": nn,
                "state": status.get("State"),
                "last_txid": int(journal["LastAppliedOrWrittenTxId"]),
                "checkpoint_txid": int(journal["MostRecentCheckpointTxId"]),
                "files": system.get("FilesTotal"),
                "blocks": system.get("BlocksTotal"),
                "safe_mode": bool(info.get("Safemode")),
            })
        except requests.RequestException:
            rows.append({"namenode": nn, "state": "caído"})
    return rows


def active_namenode():
    return next(r["namenode"] for r in namenode_states() if r["state"] == "active")


def webhdfs(path, op, method="GET", **params):
    """WebHDFS contra el NameNode activo (el standby responde 403 StandbyException)."""
    response = requests.request(method,
                                f"http://{active_namenode()}:{NAMENODE_HTTP_PORT}/webhdfs/v1{path}",
                                params={
                                    "op": op,
                                    "user.name": "hadoop",
                                    **params
                                },
                                timeout=10)
    response.raise_for_status()
    return response.json() if response.content else {}


def datanodes():
    info = jmx(active_namenode(), "Hadoop:service=NameNode,name=NameNodeInfo")
    racks = {}
    for line in hdfs("dfsadmin", "-printTopology").splitlines():
        if line.startswith("Rack:"):
            rack = line.split()[1]
        elif "(" in line:
            racks[line.split()[0]] = rack
    rows = []
    for state, field in (("vivo", "LiveNodes"), ("muerto", "DeadNodes"), ("decommission", "DecomNodes")):
        for name, node in json.loads(info[field]).items():
            address = node.get("xferaddr", "")
            rows.append({
                "datanode": name.split(":")[0],
                "state": state,
                "admin_state": node.get("adminState", "-"),
                "rack": racks.get(address, "-"),
                "address": address or "-",
                "blocks": node.get("numBlocks", "-"),
                "used_mb": round(node["used"] / 2**20, 1) if "used" in node else "-",
            })
    return rows


def fsck_blocks(path):
    """Bloques de un archivo y el DataNode (y rack) de cada réplica, según hdfs fsck -racks."""
    names = {n["address"].split(":")[0]: n["datanode"] for n in datanodes()}
    output = hdfs("fsck", path, "-files", "-blocks", "-locations", "-racks")
    rows = []
    for line in output.splitlines():
        match = re.match(r"^(\d+)\. .*:(blk_\d+)_\d+ len=(\d+) Live_repl=(\d+)\s+\[(.*)\]", line)
        if match:
            replicas = re.findall(r"(/[\w-]+)/([\d.]+):\d+", match.group(5))
            rows.append({
                "block": int(match.group(1)),
                "id": match.group(2),
                "mb": round(int(match.group(3)) / 2**20, 2),
                "replicas": int(match.group(4)),
                "where": ", ".join(f"{names.get(ip, ip)} ({rack})" for rack, ip in replicas),
            })
    return rows


# API REST de YARN contra el ResourceManager activo (el standby redirige al activo)
def active_resourcemanager():
    for rm in RESOURCEMANAGERS:
        try:
            info = requests.get(f"http://{rm}:8088/ws/v1/cluster/info", timeout=5, allow_redirects=False)
            if info.status_code == 200 and info.json()["clusterInfo"]["haState"] == "ACTIVE":
                return rm
        except requests.RequestException:
            pass
    raise RuntimeError("no hay un ResourceManager activo")


def yarn_api(path, **params):
    return requests.get(f"http://{active_resourcemanager()}:8088/ws/v1/cluster/{path}", params=params,
                        timeout=10).json()


def history_api(path):
    return requests.get(f"{JOBHISTORY_HTTP}/ws/v1/history/{path}", timeout=10).json()


def yarn_nodes():
    return [{
        "node": n["nodeHostName"],
        "rack": n["rack"],
        "state": n["state"],
        "containers": n["numContainers"],
        "used_mb": n["usedMemoryMB"],
        "available_mb": n["availMemoryMB"],
    } for n in sorted(yarn_api("nodes")["nodes"]["node"], key=lambda n: n["nodeHostName"])]


def queues():
    return [{
        "queue": q["queueName"],
        "capacity_pct": q["capacity"],
        "max_capacity_pct": q["maxCapacity"],
        "used_pct_of_cluster": round(q["absoluteUsedCapacity"], 1),
        "apps": q["numApplications"],
        "containers": q["numContainers"],
        "pending": q["pendingContainers"],
        "used_mb": q["resourcesUsed"]["memory"],
    } for q in yarn_api("scheduler")["scheduler"]["schedulerInfo"]["queues"]["queue"]]

def trigger_block_reports():
    """Pide a cada DataNode un reporte completo de sus bloques (por defecto lo envían cada 6 horas).

    Tras un failover, el nuevo NameNode activo no borra réplicas sobrantes hasta recibir un reporte completo de cada
    DataNode: su vista de qué réplicas existen podría estar desactualizada. Forzar los reportes destraba ese borrado.
    """
    from lab.config import WORKERS
    for worker in WORKERS:
        hdfs("dfsadmin", "-triggerBlockReport", f"{worker}:9867")
