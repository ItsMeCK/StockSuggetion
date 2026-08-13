"""Sector rotation agent — peer co-movement via keyword-derived sector (no
formal sector master file exists in this repo, so we group by company-name
keyword, same proxy validated in the categorization study)."""
SECTOR_KEYWORDS = {
    "Bank": "Banking", "Finance": "NBFC/Finance", "Financial": "NBFC/Finance",
    "Pharma": "Pharma", "Health": "Healthcare", "Hospital": "Healthcare",
    "Auto": "Auto", "Motors": "Auto", "Cement": "Cement", "Steel": "Metals",
    "Power": "Power", "Energy": "Energy", "Realty": "Realty", "Housing": "Housing Finance",
    "IT": "IT", "Tech": "IT", "Software": "IT", "Textile": "Textiles",
    "Chemicals": "Chemicals", "Chem": "Chemicals", "Insurance": "Insurance",
    "Consumer": "Consumer", "Foods": "FMCG", "Agro": "Agri", "Telecom": "Telecom",
}


def sector_of(company_name):
    for kw, sec in SECTOR_KEYWORDS.items():
        if kw.lower() in (company_name or "").lower():
            return sec
    return None


def build_sector_move_map(candidates_by_date, company_map, min_movers_pct=3.0):
    """candidates_by_date: {date: [(symbol, day_return_pct), ...]} for ALL
    stocks that day (or a reasonable universe subset). Returns
    {(symbol,date): True} for symbols whose sector had >=2 OTHER movers
    that day with |return| >= min_movers_pct."""
    out = {}
    for date, rows in candidates_by_date.items():
        by_sector = {}
        for sym, ret in rows:
            sec = sector_of(company_map.get(sym, ""))
            if sec and abs(ret) >= min_movers_pct:
                by_sector.setdefault(sec, []).append(sym)
        for sec, syms in by_sector.items():
            if len(syms) >= 2:
                for s in syms:
                    out[(s, date)] = len(syms)
    return out


def sector_rotation_agent(symbol, date, sector_move_map):
    n = sector_move_map.get((symbol, date))
    if n:
        return min(30 + n * 10, 70), f"{n} sector peers moving same day"
    return 0, "no sector co-movement"
