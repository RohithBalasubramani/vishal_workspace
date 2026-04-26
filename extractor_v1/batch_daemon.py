"""Batch processing daemon — runs independently of the terminal session."""
import sys, os, time, json

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_DIR)
os.chdir(PROJECT_DIR)

BATCH_FOLDER = os.path.join(PROJECT_DIR, "data", "catalogs", "batch")
LOG_FILE = "/tmp/extractor_v1_batch_daemon.log"
PID_FILE = "/tmp/extractor_v1_batch_daemon.pid"

# Write PID for tracking
with open(PID_FILE, "w") as f:
    f.write(str(os.getpid()))

from pipeline.catalog_extractor import batch_process_folder

start = time.time()
print(f"[Daemon] Started at {time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"[Daemon] PID: {os.getpid()}")
print(f"[Daemon] Folder: {BATCH_FOLDER}")
print(flush=True)

result = batch_process_folder(BATCH_FOLDER)

elapsed = time.time() - start
print(f"\n[Daemon] Finished at {time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"[Daemon] Wall time: {elapsed/60:.1f} minutes")
print(f"[Daemon] Result: {json.dumps(result, indent=2, default=str)}")

os.remove(PID_FILE)
