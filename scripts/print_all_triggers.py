import os
import sys
sys.path.append(os.getcwd())
import scripts.optimize_high_conviction as opt

def main():
    conn = opt.get_db_conn()
    # We reuse the dataset loader in optimize_high_conviction
    # (Since it queries all symbols for 2026-07-01 to 2026-07-08 and stores it in dataset)
    # We will compute it and print all triggers between July 1st and July 7th.
    
    # We just run the dataset builder. Since it queries symbols, let's let it run
    print("Building dataset...")
    # To run main loop:
    import scripts.optimize_high_conviction as hc
    hc.main()

if __name__ == "__main__":
    main()
