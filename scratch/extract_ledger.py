import re

def parse_log():
    log_file = "weekly_report_data_V4.log"
    
    with open(log_file, "r") as f:
        log_content = f.readlines()

    rescued = set()
    approved = []
    
    current_date = ""
    
    for line in log_content:
        # Extract date
        if "HISTORICAL SIMULATION FOR" in line:
            match = re.search(r"HISTORICAL SIMULATION FOR (\d{4}-\d{2}-\d{2})", line)
            if match:
                current_date = match.group(1)
        
        # Extract Rescues
        rescue_match = re.search(r"RESCUED: (\w+) \(", line)
        if rescue_match:
            symbol = rescue_match.group(1)
            rescued.add((current_date, symbol))
            
        # Extract Final Approved Allocations
        alloc_match = re.search(r"Approved Allocations: \[(.*?)\]", line)
        if alloc_match:
            symbols_str = alloc_match.group(1)
            if symbols_str:
                symbols = [s.strip(" '") for s in symbols_str.split(",")]
                for symbol in symbols:
                    if symbol:
                        # Determine which agent was key. If it's in rescued, it's MomentumAgent. Else, normal flow.
                        agent = "MomentumAgent" if (current_date, symbol) in rescued else "Standard Pipeline (Critic/Vision)"
                        approved.append((current_date, symbol, agent))

    print("| Date | Symbol | Approving Agent |")
    print("| :--- | :--- | :--- |")
    for date, sym, agent in approved:
        print(f"| {date} | **{sym}** | {agent} |")

if __name__ == "__main__":
    parse_log()
