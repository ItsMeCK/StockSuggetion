import React, { useState, useEffect } from 'react';
import { 
  Activity, 
  BarChart2, 
  Clock,
  Shield,
  Zap,
  RefreshCw
} from 'lucide-react';

interface Candidate {
  symbol: string;
  close: number;
  sma_50: number;
  sma_200: number;
  sma_50_slope_10d: number;
  score: number;
  base_score: number;
  gear_2_approved?: boolean;
  gear_2_rejection?: string;
}

interface Status {
  status: string;
  regime: string;
  last_run: string;
  last_gear_2?: string;
  candidate_count: number;
}

interface PortfolioItem {
  trade_id: string;
  ticker: string;
  status: string;
  price?: number;
  market_time?: string;
  system_time: string;
  notes?: string;
}

interface LivePrice {
  symbol: string;
  live_price: number;
  net_change: number;
  timestamp: string;
}

const App: React.FC = () => {
  const [status, setStatus] = useState<Status | null>(null);
  const [candidates, setCandidates] = useState<Candidate[]>([]);
  const [portfolio, setPortfolio] = useState<PortfolioItem[]>([]);
  const [livePrices, setLivePrices] = useState<Record<string, LivePrice>>({});
  const [loadingAll, setLoadingAll] = useState(false);

  useEffect(() => {
    const fetchInitialData = async () => {
      try {
        const [statusRes, candRes, portRes] = await Promise.all([
          fetch('http://localhost:8000/status'),
          fetch('http://localhost:8000/candidates'),
          fetch('http://localhost:8000/portfolio')
        ]);
        setStatus(await statusRes.json());
        setCandidates(await candRes.json());
        setPortfolio(await portRes.json());
      } catch (err) {
        console.error("API Error:", err);
      }
    };
    fetchInitialData();
  }, []);

  const fetchAllLivePrices = async () => {
    setLoadingAll(true);
    try {
      const res = await fetch('http://localhost:8000/live-prices');
      if (res.ok) {
        const data = await res.json();
        setLivePrices(data);
      } else {
        console.error("Failed to fetch live prices");
      }
    } catch (err) {
      console.error("API Error:", err);
    } finally {
      setLoadingAll(false);
    }
  };

  return (
    <div className="layout-container font-sans text-main bg-deep">
      {/* Sidebar */}
      <aside className="sidebar-nav">
        <div className="px-6 mb-8 flex items-center gap-3">
          <Activity size={24} className="text-accent" />
          <div>
            <h1 className="font-bold text-lg tracking-tight">SOVEREIGN</h1>
            <p className="text-[10px] text-dim uppercase">Terminal v2.1</p>
          </div>
        </div>
        
        <nav className="flex flex-col gap-1 px-4">
          <NavItem icon={<BarChart2 size={16}/>} label="Market Watch" active />
          <NavItem icon={<Shield size={16}/>} label="Risk Management" />
          <NavItem icon={<Clock size={16}/>} label="History Log" />
        </nav>
      </aside>

      {/* Main Content */}
      <main className="main-workspace">
        {/* Top Header */}
        <header className="flex justify-between items-center mb-8 pb-4 border-b">
          <div className="flex gap-8">
            <HeaderStat label="Market Regime" value={status?.regime || "UNKNOWN"} />
            <HeaderStat label="Gear 1 Signals" value={status?.candidate_count?.toString() || "0"} />
            <HeaderStat label="Gear 1 EOD Run" value={status?.last_run ? new Date(status.last_run).toLocaleTimeString() : "-"} />
            <HeaderStat label="Gear 2 Trigger" value={status?.last_gear_2 ? new Date(status.last_gear_2).toLocaleTimeString() : "-"} />
          </div>
          <div className="flex items-center gap-2">
            <div className="status-dot online"></div>
            <span className="text-xs font-semibold text-success uppercase">Engine Online</span>
          </div>
        </header>

        {/* Market Watch Table */}
        <div className="bg-card rounded-md border border-[#333]">
          <div className="px-4 py-3 border-b border-[#333] flex justify-between items-center">
            <h2 className="text-sm font-semibold">Active Candidates</h2>
            <button 
              onClick={fetchAllLivePrices}
              disabled={loadingAll}
              className="btn-live flex items-center gap-1 px-3 py-1.5 bg-accent text-white rounded border-none text-xs"
            >
              {loadingAll ? <RefreshCw size={14} className="animate-spin" /> : <Zap size={14} />}
              {Object.keys(livePrices).length > 0 ? 'Refresh Live Prices' : 'Fetch All Live Prices'}
            </button>
          </div>
          
          <table className="data-table w-full">
            <thead>
              <tr>
                <th>Instrument</th>
                <th>Conviction Score</th>
                <th>Gear 2 Entry</th>
                <th>Signal Price (EOD)</th>
                <th>50 / 200 SMA</th>
                <th>Live Price</th>
                <th>Delta (%)</th>
              </tr>
            </thead>
            <tbody>
              {candidates.map((cand, i) => {
                const live = livePrices[cand.symbol];
                
                // Calculate P&L from EOD close
                let deltaPct = 0;
                let isUp = false;
                if (live && cand.close > 0) {
                  deltaPct = ((live.live_price - cand.close) / cand.close) * 100;
                  isUp = deltaPct >= 0;
                }

                return (
                  <tr key={i}>
                    <td className="font-semibold text-[13px]">{cand.symbol}</td>
                    <td className="mono font-bold text-accent">{cand.score ? `${cand.score.toFixed(1)}/100` : '-'}</td>
                    <td className="mono">
                      {cand.gear_2_approved ? (
                        <span className="text-success font-bold uppercase text-xs bg-[#15803d33] px-2 py-1 rounded">Approved</span>
                      ) : (
                        <span className="text-danger font-bold uppercase text-xs bg-[#991b1b33] px-2 py-1 rounded" title={cand.gear_2_rejection}>Rejected</span>
                      )}
                    </td>
                    <td className="mono">₹{cand.close?.toFixed(2)}</td>
                    <td className="mono text-dim">
                      {cand.sma_50?.toFixed(1)} / {cand.sma_200?.toFixed(1)}
                    </td>
                    <td className="mono">
                       <span className={cand.sma_50_slope_10d > 0 ? 'text-success' : 'text-danger'}>
                        {cand.sma_50_slope_10d > 0 ? '+' : ''}{cand.sma_50_slope_10d?.toFixed(2)}%
                       </span>
                    </td>
                    <td className="mono font-semibold">
                      {live ? (
                        <span>₹{live.live_price.toFixed(2)}</span>
                      ) : (
                        <span className="text-dim">-</span>
                      )}
                    </td>
                    <td className="mono font-semibold">
                      {live ? (
                        <span className={isUp ? 'text-success' : 'text-danger'}>
                          {isUp ? '+' : ''}{deltaPct.toFixed(2)}%
                        </span>
                      ) : (
                        <span className="text-dim">-</span>
                      )}
                    </td>
                  </tr>
                );
              })}
              {candidates.length === 0 && (
                <tr>
                  <td colSpan={7} className="py-12 text-center text-dim text-sm">
                    No active breakout candidates available.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        {/* Bitemporal Portfolio Ledger */}
        <div className="bg-card rounded-md border border-[#333] mt-8">
          <div className="px-4 py-3 border-b border-[#333]">
            <h2 className="text-sm font-semibold">Bitemporal Position Lifecycle</h2>
          </div>
          <table className="data-table w-full">
            <thead>
              <tr>
                <th>Ticker</th>
                <th>Current Status</th>
                <th>Execution Price</th>
                <th>Market Time (Zerodha)</th>
                <th>System Record (EOD)</th>
                <th>Notes</th>
              </tr>
            </thead>
            <tbody>
              {portfolio.map((item, i) => (
                <tr key={i}>
                  <td className="font-semibold text-[13px]">{item.ticker}</td>
                  <td>
                    <span className={`status-badge ${item.status.toLowerCase()}`}>
                      {item.status}
                    </span>
                  </td>
                  <td className="mono">
                    {item.price ? `₹${item.price.toFixed(2)}` : '-'}
                  </td>
                  <td className="mono text-xs text-dim">
                    {item.market_time ? new Date(item.market_time).toLocaleString() : 'Intraday Pending'}
                  </td>
                  <td className="mono text-xs text-dim">
                    {new Date(item.system_time).toLocaleString()}
                  </td>
                  <td className="text-xs text-dim">{item.notes || '-'}</td>
                </tr>
              ))}
              {portfolio.length === 0 && (
                <tr>
                  <td colSpan={6} className="py-8 text-center text-dim text-sm">
                    No live lifecycle events recorded in the ledger.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </main>
    </div>
  );
};

const NavItem = ({ icon, label, active = false }: { icon: React.ReactNode, label: string, active?: boolean }) => (
  <div className={`flex items-center gap-3 px-3 py-2.5 rounded-md cursor-pointer transition-colors text-sm ${active ? 'bg-[#2d2d2d] text-white font-semibold' : 'text-dim hover:bg-[#1f1f1f] hover:text-white'}`}>
    <div className={active ? 'text-accent' : 'text-dim'}>{icon}</div>
    {label}
  </div>
);

const HeaderStat = ({ label, value }: { label: string, value: string }) => (
  <div>
    <p className="text-[10px] text-dim uppercase mb-1">{label}</p>
    <p className="text-sm font-semibold">{value}</p>
  </div>
);

export default App;
