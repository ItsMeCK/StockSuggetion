"""
40-Industry Demand-Supply Cycle Tracker
Requirements EQ32, EQ33, EQ39
Synthesized from 100-Agent Study K01-K40
"""

from datetime import date
from enum import Enum
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

class CyclePhase(Enum):
    SUPPLY_SQUEEZED = "SUPPLY_SQUEEZED"   # Capacity utilization > 75%, multi-year order backlogs, pricing power (Overweight)
    EXPANDING = "EXPANDING"               # Healthy volume demand growth, disciplined capex (Selective Growth)
    MATURE = "MATURE"                     # High penetration, ARPU/pricing-led, steady cash flows (Neutral / Core)
    OVERSUPPLIED = "OVERSUPPLIED"         # Aggressive capacity addition, Chinese dumping, margin contraction (Avoid)

@dataclass(frozen=True)
class IndustryDemandSupplyProfile:
    industry_name: str
    cycle_phase: CyclePhase
    capacity_utilization_pct: float
    pricing_power: str         # "High", "Moderate", "Weak"
    capex_intensity: str       # "Heavy", "Disciplined", "Low"
    rationale: str
    effective_from_date: date = date(2024, 4, 1) # Dated point-in-time effectiveness (EQ39)

# Master 40-Industry Demand-Supply Database (K01 to K40)
INDUSTRY_CATALOG: Dict[str, IndustryDemandSupplyProfile] = {
    # 1-10: Capital Goods, Power, Infrastructure & Defence
    "Power Generation & Utilities": IndustryDemandSupplyProfile(
        "Power Generation & Utilities", CyclePhase.SUPPLY_SQUEEZED, 78.5, "High", "Disciplined",
        "Peak deficit during summer, long gestation for thermal/nuclear additions"
    ),
    "Power T&D & Transformers": IndustryDemandSupplyProfile(
        "Power T&D & Transformers", CyclePhase.SUPPLY_SQUEEZED, 85.0, "High", "Heavy",
        "Global transformer lead times 3-4 years, HVDC substation buildouts"
    ),
    "Defence Equipment & Aerospace": IndustryDemandSupplyProfile(
        "Defence Equipment & Aerospace", CyclePhase.SUPPLY_SQUEEZED, 82.0, "High", "Disciplined",
        "Make-in-India mandates, multi-year sovereign order books, export clearance"
    ),
    "Shipbuilding & Marine": IndustryDemandSupplyProfile(
        "Shipbuilding & Marine", CyclePhase.SUPPLY_SQUEEZED, 88.0, "High", "Disciplined",
        "Dry-dock slots booked through 2028, commercial fleet replacement"
    ),
    "EMS & Semiconductor Assembly": IndustryDemandSupplyProfile(
        "EMS & Semiconductor Assembly", CyclePhase.SUPPLY_SQUEEZED, 80.0, "High", "Heavy",
        "Mobile/industrial PLI schemes, electronics localization drive"
    ),
    "Renewable & Solar Components": IndustryDemandSupplyProfile(
        "Renewable & Solar Components", CyclePhase.EXPANDING, 72.0, "Moderate", "Heavy",
        "ALMM module mandate protections against Chinese cell imports"
    ),
    "Railway Infrastructure & Rolling Stock": IndustryDemandSupplyProfile(
        "Railway Infrastructure & Rolling Stock", CyclePhase.SUPPLY_SQUEEZED, 81.0, "High", "Disciplined",
        "Dedicated Freight Corridor and Vande Bharat wagon/track allocation"
    ),
    "Heavy Electricals & Boilers": IndustryDemandSupplyProfile(
        "Heavy Electricals & Boilers", CyclePhase.SUPPLY_SQUEEZED, 77.0, "High", "Disciplined",
        "Thermal revival orders from NTPC and state gencos"
    ),
    "Ports & Marine Logistics": IndustryDemandSupplyProfile(
        "Ports & Marine Logistics", CyclePhase.EXPANDING, 74.0, "High", "Disciplined",
        "Containerization growth and DFC coastal connectivity"
    ),
    "Roads & Highways EPC": IndustryDemandSupplyProfile(
        "Roads & Highways EPC", CyclePhase.MATURE, 68.0, "Moderate", "Low",
        "HAM project execution steady, NHAI awarding moderation"
    ),

    # 11-20: Financials, Banking & NBFCs
    "Private Sector Large Banks": IndustryDemandSupplyProfile(
        "Private Sector Large Banks", CyclePhase.MATURE, 72.0, "Moderate", "Disciplined",
        "Deposit mobilization constraints, elevated CD ratios normalizing"
    ),
    "Public Sector Banks": IndustryDemandSupplyProfile(
        "Public Sector Banks", CyclePhase.EXPANDING, 75.0, "Moderate", "Disciplined",
        "Clean balance sheets, corporate capex revival lending"
    ),
    "Housing Finance Companies": IndustryDemandSupplyProfile(
        "Housing Finance Companies", CyclePhase.EXPANDING, 74.0, "Moderate", "Disciplined",
        "Affordable and prime housing demand strong, stable asset quality"
    ),
    "Vehicle Finance NBFCs": IndustryDemandSupplyProfile(
        "Vehicle Finance NBFCs", CyclePhase.MATURE, 70.0, "Moderate", "Disciplined",
        "Rural cash flows mixed, commercial vehicle cycle peaking"
    ),
    "Gold Loan NBFCs": IndustryDemandSupplyProfile(
        "Gold Loan NBFCs", CyclePhase.EXPANDING, 76.0, "High", "Disciplined",
        "High gold prices driving collateral valuation and loan book growth"
    ),
    "Wealth Management & Broking": IndustryDemandSupplyProfile(
        "Wealth Management & Broking", CyclePhase.EXPANDING, 73.0, "Moderate", "Low",
        "Financialization of domestic savings, mutual fund SIP inflows"
    ),
    "Life & General Insurance": IndustryDemandSupplyProfile(
        "Life & General Insurance", CyclePhase.MATURE, 65.0, "Moderate", "Low",
        "Tax changes in high-ticket policies, steady retail term/health expansion"
    ),
    "Microfinance Institutions": IndustryDemandSupplyProfile(
        "Microfinance Institutions", CyclePhase.OVERSUPPLIED, 60.0, "Weak", "Heavy",
        "Over-indebtedness in rural/semi-urban belts, rising credit costs"
    ),
    "Infrastructure Finance NBFCs": IndustryDemandSupplyProfile(
        "Infrastructure Finance NBFCs", CyclePhase.SUPPLY_SQUEEZED, 80.0, "High", "Disciplined",
        "Monopolistic transmission and generation financing lines"
    ),
    "Asset Reconstruction & Distressed Debt": IndustryDemandSupplyProfile(
        "Asset Reconstruction & Distressed Debt", CyclePhase.MATURE, 66.0, "Moderate", "Low",
        "Resolution momentum under IBC framework"
    ),

    # 21-30: Technology, Healthcare & Consumer
    "IT Services (Large Cap)": IndustryDemandSupplyProfile(
        "IT Services (Large Cap)", CyclePhase.EXPANDING, 74.0, "Moderate", "Low",
        "Discretionary spending recovery, cloud and enterprise AI migration"
    ),
    "IT Services (Mid Cap Tier-2)": IndustryDemandSupplyProfile(
        "IT Services (Mid Cap Tier-2)", CyclePhase.EXPANDING, 78.0, "High", "Low",
        "Vertical specialization in automotive ER&D, healthcare, and BFS"
    ),
    "Domestic Formulations Pharma": IndustryDemandSupplyProfile(
        "Domestic Formulations Pharma", CyclePhase.EXPANDING, 76.0, "High", "Disciplined",
        "Chronic therapies growth, domestic brand stickiness, NLEM resilience"
    ),
    "US Generics Pharma": IndustryDemandSupplyProfile(
        "US Generics Pharma", CyclePhase.MATURE, 65.0, "Weak", "Disciplined",
        "Price erosion stabilizing but FDA compliance scrutiny remains active"
    ),
    "CDMO & API Synthesis": IndustryDemandSupplyProfile(
        "CDMO & API Synthesis", CyclePhase.EXPANDING, 75.0, "Moderate", "Heavy",
        "Biosecure Act supply chain diversification out of China"
    ),
    "Hospitals & Healthcare Services": IndustryDemandSupplyProfile(
        "Hospitals & Healthcare Services", CyclePhase.SUPPLY_SQUEEZED, 82.0, "High", "Disciplined",
        "Bed occupancy > 70%, ARPOB expansion, brownfield capex discipline"
    ),
    "FMCG Staples & Foods": IndustryDemandSupplyProfile(
        "FMCG Staples & Foods", CyclePhase.MATURE, 68.0, "Moderate", "Low",
        "Rural recovery gradual, local unorganized competition in edible oils/biscuits"
    ),
    "Paints & Decorative Coatings": IndustryDemandSupplyProfile(
        "Paints & Decorative Coatings", CyclePhase.OVERSUPPLIED, 58.0, "Weak", "Heavy",
        "Grasim Birla Opus massive capacity entry pressuring industry gross margins"
    ),
    "QSR & Food Retail": IndustryDemandSupplyProfile(
        "QSR & Food Retail", CyclePhase.MATURE, 62.0, "Weak", "Heavy",
        "Same-store sales growth muted, rapid store expansion cannibalization"
    ),
    "Apparel & Fashion Retail": IndustryDemandSupplyProfile(
        "Apparel & Fashion Retail", CyclePhase.MATURE, 64.0, "Moderate", "Disciplined",
        "Value fashion (Zudio/Yousta) strong, premium lifestyle moderating"
    ),

    # 31-40: Commodities, Materials, Telecom, Energy & Auto
    "Automobile OEMs (PV & 2W)": IndustryDemandSupplyProfile(
        "Automobile OEMs (PV & 2W)", CyclePhase.MATURE, 71.0, "Moderate", "Disciplined",
        "Dealership inventory normalization, SUV mix premiumization"
    ),
    "Auto Ancillaries & Precision Engineering": IndustryDemandSupplyProfile(
        "Auto Ancillaries & Precision Engineering", CyclePhase.EXPANDING, 76.0, "Moderate", "Disciplined",
        "EV component transition, global export sourcing from India"
    ),
    "Telecom Services": IndustryDemandSupplyProfile(
        "Telecom Services", CyclePhase.SUPPLY_SQUEEZED, 84.0, "High", "Low",
        "3-player market oligopoly, recurring tariff hike pricing power"
    ),
    "Cement (Southern Region)": IndustryDemandSupplyProfile(
        "Cement (Southern Region)", CyclePhase.OVERSUPPLIED, 55.0, "Weak", "Heavy",
        "Excess limestone grinding capacity, fierce price discounting"
    ),
    "Cement (Northern & Central)": IndustryDemandSupplyProfile(
        "Cement (Northern & Central)", CyclePhase.EXPANDING, 75.0, "Moderate", "Disciplined",
        "Higher utilization, consolidated regional market structure"
    ),
    "Steel & Ferrous Metals": IndustryDemandSupplyProfile(
        "Steel & Ferrous Metals", CyclePhase.OVERSUPPLIED, 64.0, "Weak", "Heavy",
        "Chinese steel export dumping pressuring domestic flat/long realizations"
    ),
    "Specialty & Fluorochemicals": IndustryDemandSupplyProfile(
        "Specialty & Fluorochemicals", CyclePhase.EXPANDING, 73.0, "Moderate", "Heavy",
        "Agrochemical destocking bottoming out, refrigerant gas transition"
    ),
    "Bulk Commodity Chemicals": IndustryDemandSupplyProfile(
        "Bulk Commodity Chemicals", CyclePhase.OVERSUPPLIED, 52.0, "Weak", "Heavy",
        "Global petrochemical oversupply, depressed spreads over naphtha"
    ),
    "City Gas Distribution (CGD)": IndustryDemandSupplyProfile(
        "City Gas Distribution (CGD)", CyclePhase.MATURE, 69.0, "Moderate", "Disciplined",
        "APM gas allocation reductions shifting input mix to imported LNG"
    ),
    "Commercial Real Estate & SEZs": IndustryDemandSupplyProfile(
        "Commercial Real Estate & SEZs", CyclePhase.EXPANDING, 77.0, "High", "Disciplined",
        "Global Capability Centres (GCC) leasing driving Grade-A office absorption"
    )
}

# Sector name normalizer for NSE symbol matching
SYMBOL_SECTOR_MAP: Dict[str, str] = {
    # Power & T&D
    "NTPC": "Power Generation & Utilities",
    "POWERGRID": "Power Generation & Utilities",
    "TATAPOWER": "Power Generation & Utilities",
    "BHEL": "Heavy Electricals & Boilers",
    "SIEMENS": "Power T&D & Transformers",
    "ABB": "Power T&D & Transformers",
    "APARINDS": "Power T&D & Transformers",
    "VOLTAMP": "Power T&D & Transformers",
    "TRITURBINE": "Power Generation & Utilities",
    
    # Defence & Ship
    "HAL": "Defence Equipment & Aerospace",
    "BEL": "Defence Equipment & Aerospace",
    "MAZDOCK": "Shipbuilding & Marine",
    "COCHINSHIP": "Shipbuilding & Marine",
    "BDL": "Defence Equipment & Aerospace",
    
    # EMS / Tech
    "KAYNES": "EMS & Semiconductor Assembly",
    "DIXON": "EMS & Semiconductor Assembly",
    "SYRMA": "EMS & Semiconductor Assembly",
    "TCS": "IT Services (Large Cap)",
    "INFY": "IT Services (Large Cap)",
    "HCLTECH": "IT Services (Large Cap)",
    "WIPRO": "IT Services (Large Cap)",
    "KPITTECH": "IT Services (Mid Cap Tier-2)",
    "TATAELXSI": "IT Services (Mid Cap Tier-2)",
    "LTM": "IT Services (Mid Cap Tier-2)",
    "PERSISTENT": "IT Services (Mid Cap Tier-2)",
    "COFORGE": "IT Services (Mid Cap Tier-2)",

    # Healthcare
    "APOLLOHOSP": "Hospitals & Healthcare Services",
    "MAXHEALTH": "Hospitals & Healthcare Services",
    "FORTIS": "Hospitals & Healthcare Services",
    "SUNPHARMA": "Domestic Formulations Pharma",
    "CIPLA": "Domestic Formulations Pharma",
    "MANKIND": "Domestic Formulations Pharma",
    "DRREDDY": "US Generics Pharma",
    "LUPIN": "US Generics Pharma",
    "DIVISLAB": "CDMO & API Synthesis",
    "SYNGENE": "CDMO & API Synthesis",

    # Telecom & Ports
    "BHARTIARTL": "Telecom Services",
    "ADANIPORTS": "Ports & Marine Logistics",

    # Banking & NBFCs
    "HDFCBANK": "Private Sector Large Banks",
    "ICICIBANK": "Private Sector Large Banks",
    "AXISBANK": "Private Sector Large Banks",
    "KOTAKBANK": "Private Sector Large Banks",
    "SBIN": "Public Sector Banks",
    "BANKBARODA": "Public Sector Banks",
    "CANBK": "Public Sector Banks",
    "BAJFINANCE": "Vehicle Finance NBFCs",
    "CHOLAFIN": "Vehicle Finance NBFCs",
    "MUTHOOTFIN": "Gold Loan NBFCs",
    "MANAPPURAM": "Gold Loan NBFCs",
    "PFC": "Infrastructure Finance NBFCs",
    "RECLTD": "Infrastructure Finance NBFCs",

    # Metals / Cement / Paints (Vulnerable to Overcapacity)
    "TATASTEEL": "Steel & Ferrous Metals",
    "JSWSTEEL": "Steel & Ferrous Metals",
    "SAIL": "Steel & Ferrous Metals",
    "JINDALSTEL": "Steel & Ferrous Metals",
    "ASIANPAINT": "Paints & Decorative Coatings",
    "BERGEPAINT": "Paints & Decorative Coatings",
    "RAMCOCEM": "Cement (Southern Region)",
    "INDIACEM": "Cement (Southern Region)",
    "ULTRACEMCO": "Cement (Northern & Central)",
    "AMBUJACEM": "Cement (Northern & Central)"
}

class IndustryCycleTracker:
    """
    Step 3 Engine: Evaluates industry demand-supply dynamics.
    Enforces tactical tilts:
    - SUPPLY_SQUEEZED: +10% conviction bonus, preferred weighting
    - EXPANDING: +5% conviction bonus
    - MATURE: 0% neutral
    - OVERSUPPLIED: -20% penalty, strict sizing cap (max 2.5%) or veto
    """
    def __init__(self, catalog: Optional[Dict[str, IndustryDemandSupplyProfile]] = None):
        self.catalog = catalog or INDUSTRY_CATALOG

    def get_profile_for_industry(self, industry_name: str) -> Optional[IndustryDemandSupplyProfile]:
        return self.catalog.get(industry_name)

    def get_profile_for_symbol(self, symbol: str) -> Optional[IndustryDemandSupplyProfile]:
        industry_name = SYMBOL_SECTOR_MAP.get(symbol.upper())
        if industry_name:
            return self.get_profile_for_industry(industry_name)
        return None

    def evaluate_cycle(
        self,
        symbol: str,
        default_industry: str = "IT Services (Large Cap)",
        as_of_date: Optional[date] = None
    ) -> Tuple[CyclePhase, float, str]:
        profile = self.get_profile_for_symbol(symbol)
        if profile is None:
            profile = self.get_profile_for_industry(default_industry)
            if profile is None:
                return CyclePhase.MATURE, 0.0, "Generic default industry (Mature)"

        # Point-in-Time Check (EQ39): If evaluating before profile was published/effective, fail-safe to MATURE (neutral)
        if as_of_date is not None and as_of_date < profile.effective_from_date:
            return CyclePhase.MATURE, 0.0, f"{profile.industry_name} profile effective {profile.effective_from_date}; prior date {as_of_date} defaults to MATURE (No Look-Ahead)"

        phase = profile.cycle_phase
        if phase == CyclePhase.SUPPLY_SQUEEZED:
            weight_bonus = 10.0
            verdict = f"{profile.industry_name} is SUPPLY-SQUEEZED (Cap Util {profile.capacity_utilization_pct:.1f}%, Pricing: {profile.pricing_power}). Overweight."
        elif phase == CyclePhase.EXPANDING:
            weight_bonus = 5.0
            verdict = f"{profile.industry_name} is EXPANDING (Demand growth solid, capex disciplined). Selective Growth."
        elif phase == CyclePhase.MATURE:
            weight_bonus = 0.0
            verdict = f"{profile.industry_name} is MATURE (Cash cow/stable margins). Neutral baseline."
        else: # OVERSUPPLIED
            weight_bonus = -20.0
            verdict = f"{profile.industry_name} is OVERSUPPLIED (Util {profile.capacity_utilization_pct:.1f}%, severe supply additions/dumping). Underweight/Avoid."

        return phase, weight_bonus, verdict
