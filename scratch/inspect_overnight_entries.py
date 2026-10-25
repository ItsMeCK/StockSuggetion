import pandas as pd

df = pd.read_csv('data/grounded_201_trades_audit.csv')
df['entry_date'] = df['entry_time_ist'].str[:10]
df['exit_date'] = df['exit_time_ist'].str[:10]
df['is_overnight'] = df['entry_date'] != df['exit_date']
df['entry_time'] = df['entry_time_ist'].str[11:16]

print("=== ALL 201 TRADES BY ENTRY TIME ===")
all_summary = df.groupby('entry_time').agg(
    total_trades=('net_pnl_inr', 'count'),
    overnight_trades=('is_overnight', 'sum'),
    sameday_trades=('is_overnight', lambda x: (~x).sum()),
    total_net_pnl=('net_pnl_inr', 'sum'),
    avg_net_pnl=('net_pnl_inr', 'mean')
)
print(all_summary)

print("\n=== OVERNIGHT (80 TRADES) BY ENTRY TIME ===")
overnight_df = df[df['is_overnight']]
ov_summary = overnight_df.groupby('entry_time').agg(
    count=('net_pnl_inr', 'count'),
    win_rate=('net_pnl_inr', lambda x: (x > 0).mean() * 100),
    total_net_pnl=('net_pnl_inr', 'sum'),
    avg_net_pnl=('net_pnl_inr', 'mean')
)
print(ov_summary)

print("\n=== SAME DAY (121 TRADES) BY ENTRY TIME ===")
sameday_df = df[~df['is_overnight']]
sd_summary = sameday_df.groupby('entry_time').agg(
    count=('net_pnl_inr', 'count'),
    win_rate=('net_pnl_inr', lambda x: (x > 0).mean() * 100),
    total_net_pnl=('net_pnl_inr', 'sum'),
    avg_net_pnl=('net_pnl_inr', 'mean')
)
print(sd_summary)
