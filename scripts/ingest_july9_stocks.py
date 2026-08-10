import os
import sys

sys.path.append(os.getcwd())

from pipeline.ingestion import run_eod_ingestion

if __name__ == "__main__":
    print("Starting EOD stock ingestion for July 9th...")
    run_eod_ingestion()
    print("EOD Ingestion completed.")
