import os
import re
import subprocess
import time

from lab.hdfs import hdfs

STREAMING = "/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.5.0.jar"


def counters(log):
    """Los contadores que el cliente imprime al terminar un trabajo ("Nombre=valor" por línea)."""
    return {name.strip(): int(value) for name, value in re.findall(r"^\s+([A-Za-z][^=\n]+)=(\d+)$", log, re.M)}


def job_id(log):
    match = re.search(r"Running job: (job_\d+_\d+)", log)
    return match.group(1) if match else None


def submit(command, check=True):
    """Ejecuta un trabajo (mapred streaming, hadoop jar...) y devuelve su log, contadores, id y duración."""
    started = time.perf_counter()
    # Con nivel INFO, el cliente imprime el progreso y, al final, los contadores del trabajo (por stderr)
    job = subprocess.run(command, capture_output=True, text=True,
                         env={**os.environ, "HADOOP_ROOT_LOGGER": "INFO,console"})
    log = job.stdout + job.stderr
    if check and job.returncode != 0:
        raise RuntimeError(log[-3000:])
    return {
        "ok": job.returncode == 0,
        "seconds": round(time.perf_counter() - started, 1),
        "job": job_id(log),
        "counters": counters(log),
        "log": log
    }


def streaming(name, input, output, mapper, reducer=None, files=(), generic=(), options=(), check=True):
    """Hadoop Streaming: mapper y reducer son comandos que leen stdin y escriben stdout (aquí, scripts de Python)."""
    hdfs("dfs", "-rm", "-r", "-f", "-skipTrash", output)
    command = ["mapred", "streaming", "-D", f"mapreduce.job.name={name}", *generic]
    if files:
        command += ["-files", ",".join(files)]
    command += ["-input", input, "-output", output, "-mapper", mapper, *options]
    command += ["-reducer", reducer] if reducer else ["-numReduceTasks", "0"]
    return submit(command, check=check)
