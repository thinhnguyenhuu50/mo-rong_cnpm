import os
import shutil
from pathlib import Path

from diagrams import Diagram, Cluster
from diagrams.onprem.client import User
from diagrams.onprem.compute import Server
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.inmemory import Redis
from diagrams.onprem.queue import Celery
from diagrams.onprem.network import Internet


def ensure_graphviz_on_path() -> None:
    if shutil.which("dot"):
        return

    # Common Graphviz install locations on Windows.
    candidates = [
        Path(os.environ.get("PROGRAMFILES", r"C:\\Program Files")) / "Graphviz" / "bin",
        Path(os.environ.get("PROGRAMFILES(X86)", r"C:\\Program Files (x86)")) / "Graphviz" / "bin",
    ]

    for bin_dir in candidates:
        if (bin_dir / "dot.exe").exists():
            os.environ["PATH"] = f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}"
            return

    raise RuntimeError(
        "Graphviz 'dot' executable not found. Install Graphviz and ensure its bin "
        "folder is on PATH (e.g., C:\\Program Files\\Graphviz\\bin)."
    )

# Define the diagram attributes
graph_attr = {
    "fontsize": "20",
    "pad": "0.5"
}

ensure_graphviz_on_path()

with Diagram("SERS High-Level Architecture", show=False, graph_attr=graph_attr):
    user = User("End-User Interface")
    external_smtp = Internet("Notification Service (SMTP/SMS)")

    with Cluster("MVC Synchronous Layer"):
        controller = Server("Flask Web Server\n(Controller & View routing)")
        database = PostgreSQL("SQLite Database\n(Model Layer)")

    with Cluster("Asynchronous Execution Layer"):
        redis_broker = Redis("Redis\n(Message Broker)")
        celery_worker = Celery("Celery\n(Background Worker)")

    # 1. Synchronous CRUD operations
    user >> controller
    controller >> database
    
    # 2. Asynchronous Event Queuing
    controller >> redis_broker
    
    # 3. Background Task Processing
    redis_broker >> celery_worker
    
    # 4. Final Notification Dispatch
    celery_worker >> external_smtp