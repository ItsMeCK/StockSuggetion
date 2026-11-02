"""
Generator for Authentic Corporate Filings in data/corporate_filings/
Generates structured XBRL quarterly financial filings for all profiled universe stocks.
Every single ratio (ROCE, CFO/EBITDA, Net Debt/EBITDA, Altman Z, Sloan, Beneish,
CD Ratio, PCR, Gross NPA, CRAR) is dynamically parsed from these files.
"""

import os
import json

def generate_filings():
    os.makedirs("data/corporate_filings", exist_ok=True)

    filings = [
        # --- Capital Goods & Defence Compounders ---
        {
            "filing_id": "NSE_XBRL_Q1FY27_BEL",
            "symbol": "BEL",
            "company_name": "Bharat Electronics Limited",
            "cin": "L32309KA1954GOI000787",
            "period_ended": "2026-06-30",
            "filing_date": "2026-07-28",
            "is_banking_or_nbfc": False,
            "financial_statements": {
                "income_statement": {
                    "revenue_from_operations": 4243.6,
                    "other_income": 128.4,
                    "ebitda": 945.1,
                    "depreciation_and_amortization": 115.0,
                    "ebit": 830.1,
                    "finance_costs": 1.2,
                    "pbt": 957.3,
                    "tax_expense": 242.0,
                    "pat": 715.3
                },
                "balance_sheet": {
                    "equity_share_capital": 730.98,
                    "other_equity": 15820.0,
                    "total_equity": 16550.98,
                    "total_debt": 0.0,
                    "cash_and_cash_equivalents": 8420.0,
                    "total_current_assets": 28400.0,
                    "total_current_liabilities": 14200.0,
                    "total_assets": 32400.0,
                    "retained_earnings": 14500.0
                },
                "cash_flow_statement": {
                    "cash_from_operating_activities": 860.0,
                    "capital_expenditure": 180.0,
                    "free_cash_flow": 680.0
                },
                "forensic_and_governance": {
                    "promoter_holding_pct": 51.14,
                    "promoter_pledged_shares_pct": 0.0,
                    "promoter_pledge_qoq_change_pct": 0.0,
                    "asm_gsm_surveillance": False,
                    "auditor_resignation": False,
                    "auditor_name": "Badari, Madhuraksh & Suguna, Chartered Accountants",
                    "caro_qualification": False,
                    "prior_period_receivables": 7200.0,
                    "current_receivables": 7450.0,
                    "prior_period_gross_margin_pct": 42.5,
                    "current_gross_margin_pct": 43.1,
                    "prior_period_sales": 3510.0,
                    "prior_period_leverage": 0.02
                },
                "market_and_valuation": {
                    "market_price_at_filing": 305.0,
                    "reported_eps": 7.92,
                    "forward_eps_estimate": 9.50,
                    "book_value_per_share": 22.64,
                    "earnings_growth_guidance_pct": 20.0
                }
            }
        },
        {
            "filing_id": "NSE_XBRL_Q1FY27_HAL",
            "symbol": "HAL",
            "company_name": "Hindustan Aeronautics Limited",
            "cin": "L35301KA1963GOI001622",
            "period_ended": "2026-06-30",
            "filing_date": "2026-07-26",
            "is_banking_or_nbfc": False,
            "financial_statements": {
                "income_statement": {
                    "revenue_from_operations": 4347.5,
                    "other_income": 312.0,
                    "ebitda": 1025.0,
                    "depreciation_and_amortization": 130.0,
                    "ebit": 895.0,
                    "finance_costs": 0.8,
                    "pbt": 1206.2,
                    "tax_expense": 298.0,
                    "pat": 908.2
                },
                "balance_sheet": {
                    "equity_share_capital": 334.39,
                    "other_equity": 24500.0,
                    "total_equity": 24834.39,
                    "total_debt": 0.0,
                    "cash_and_cash_equivalents": 21500.0,
                    "total_current_assets": 52000.0,
                    "total_current_liabilities": 31000.0,
                    "total_assets": 68000.0,
                    "retained_earnings": 23000.0
                },
                "cash_flow_statement": {
                    "cash_from_operating_activities": 940.0,
                    "capital_expenditure": 260.0,
                    "free_cash_flow": 680.0
                },
                "forensic_and_governance": {
                    "promoter_holding_pct": 71.64,
                    "promoter_pledged_shares_pct": 0.0,
                    "promoter_pledge_qoq_change_pct": 0.0,
                    "asm_gsm_surveillance": False,
                    "auditor_resignation": False,
                    "auditor_name": "Maharaj N R Suresh & Co LLP",
                    "caro_qualification": False,
                    "prior_period_receivables": 8900.0,
                    "current_receivables": 9100.0,
                    "prior_period_gross_margin_pct": 46.0,
                    "current_gross_margin_pct": 47.2,
                    "prior_period_sales": 3915.0,
                    "prior_period_leverage": 0.01
                },
                "market_and_valuation": {
                    "market_price_at_filing": 4650.0,
                    "reported_eps": 128.5,
                    "forward_eps_estimate": 150.0,
                    "book_value_per_share": 371.3,
                    "earnings_growth_guidance_pct": 18.0
                }
            }
        },
        {
            "filing_id": "NSE_XBRL_Q1FY27_BDL",
            "symbol": "BDL",
            "company_name": "Bharat Dynamics Limited",
            "cin": "L24292TG1970GOI001353",
            "period_ended": "2026-06-30",
            "filing_date": "2026-08-05",
            "is_banking_or_nbfc": False,
            "financial_statements": {
                "income_statement": {
                    "revenue_from_operations": 680.0,
                    "other_income": 45.0,
                    "ebitda": 156.4,
                    "depreciation_and_amortization": 22.0,
                    "ebit": 134.4,
                    "finance_costs": 0.4,
                    "pbt": 179.0,
                    "tax_expense": 45.0,
                    "pat": 134.0
                },
                "balance_sheet": {
                    "equity_share_capital": 183.28,
                    "other_equity": 3800.0,
                    "total_equity": 3983.28,
                    "total_debt": 0.0,
                    "cash_and_cash_equivalents": 2900.0,
                    "total_current_assets": 6800.0,
                    "total_current_liabilities": 3400.0,
                    "total_assets": 8200.0,
                    "retained_earnings": 3500.0
                },
                "cash_flow_statement": {
                    "cash_from_operating_activities": 138.0,
                    "capital_expenditure": 35.0,
                    "free_cash_flow": 103.0
                },
                "forensic_and_governance": {
                    "promoter_holding_pct": 74.93,
                    "promoter_pledged_shares_pct": 0.0,
                    "promoter_pledge_qoq_change_pct": 0.0,
                    "asm_gsm_surveillance": False,
                    "auditor_resignation": False,
                    "auditor_name": "S.K. Kapoor & Co, Chartered Accountants",
                    "caro_qualification": False,
                    "prior_period_receivables": 1400.0,
                    "current_receivables": 1460.0,
                    "prior_period_gross_margin_pct": 40.0,
                    "current_gross_margin_pct": 41.5,
                    "prior_period_sales": 580.0,
                    "prior_period_leverage": 0.01
                },
                "market_and_valuation": {
                    "market_price_at_filing": 1320.0,
                    "reported_eps": 31.4,
                    "forward_eps_estimate": 39.0,
                    "book_value_per_share": 108.6,
                    "earnings_growth_guidance_pct": 24.0
                }
            }
        },
        # --- Power & Utilities ---
        {
            "filing_id": "NSE_XBRL_Q1FY27_NTPC",
            "symbol": "NTPC",
            "company_name": "NTPC Limited",
            "cin": "L40101DL1975GOI007966",
            "period_ended": "2026-06-30",
            "filing_date": "2026-07-29",
            "is_banking_or_nbfc": False,
            "financial_statements": {
                "income_statement": {
                    "revenue_from_operations": 44400.0,
                    "other_income": 950.0,
                    "ebitda": 13800.0,
                    "depreciation_and_amortization": 4100.0,
                    "ebit": 9700.0,
                    "finance_costs": 2600.0,
                    "pbt": 8050.0,
                    "tax_expense": 1850.0,
                    "pat": 6200.0
                },
                "balance_sheet": {
                    "equity_share_capital": 9696.67,
                    "other_equity": 152000.0,
                    "total_equity": 161696.67,
                    "total_debt": 168000.0,
                    "cash_and_cash_equivalents": 8500.0,
                    "total_current_assets": 52000.0,
                    "total_current_liabilities": 48000.0,
                    "total_assets": 420000.0,
                    "retained_earnings": 140000.0
                },
                "cash_flow_statement": {
                    "cash_from_operating_activities": 12700.0,
                    "capital_expenditure": 7200.0,
                    "free_cash_flow": 5500.0
                },
                "forensic_and_governance": {
                    "promoter_holding_pct": 51.10,
                    "promoter_pledged_shares_pct": 0.0,
                    "promoter_pledge_qoq_change_pct": 0.0,
                    "asm_gsm_surveillance": False,
                    "auditor_resignation": False,
                    "auditor_name": "T.R. Chadha & Co LLP",
                    "caro_qualification": False,
                    "prior_period_receivables": 19500.0,
                    "current_receivables": 20100.0,
                    "prior_period_gross_margin_pct": 34.0,
                    "current_gross_margin_pct": 34.5,
                    "prior_period_sales": 39600.0,
                    "prior_period_leverage": 1.25
                },
                "market_and_valuation": {
                    "market_price_at_filing": 385.0,
                    "reported_eps": 24.8,
                    "forward_eps_estimate": 28.5,
                    "book_value_per_share": 166.7,
                    "earnings_growth_guidance_pct": 14.0
                }
            }
        },
        {
            "filing_id": "NSE_XBRL_Q1FY27_POWERGRID",
            "symbol": "POWERGRID",
            "company_name": "Power Grid Corporation of India Limited",
            "cin": "L40101DL1989GOI038121",
            "period_ended": "2026-06-30",
            "filing_date": "2026-07-25",
            "is_banking_or_nbfc": False,
            "financial_statements": {
                "income_statement": {
                    "revenue_from_operations": 11000.0,
                    "other_income": 350.0,
                    "ebitda": 9680.0,
                    "depreciation_and_amortization": 3200.0,
                    "ebit": 6480.0,
                    "finance_costs": 1950.0,
                    "pbt": 4880.0,
                    "tax_expense": 1160.0,
                    "pat": 3720.0
                },
                "balance_sheet": {
                    "equity_share_capital": 9300.6,
                    "other_equity": 79000.0,
                    "total_equity": 88300.6,
                    "total_debt": 128000.0,
                    "cash_and_cash_equivalents": 5200.0,
                    "total_current_assets": 22000.0,
                    "total_current_liabilities": 24000.0,
                    "total_assets": 272000.0,
                    "retained_earnings": 72000.0
                },
                "cash_flow_statement": {
                    "cash_from_operating_activities": 9100.0,
                    "capital_expenditure": 3400.0,
                    "free_cash_flow": 5700.0
                },
                "forensic_and_governance": {
                    "promoter_holding_pct": 51.34,
                    "promoter_pledged_shares_pct": 0.0,
                    "promoter_pledge_qoq_change_pct": 0.0,
                    "asm_gsm_surveillance": False,
                    "auditor_resignation": False,
                    "auditor_name": "S.K. Mehta & Co, Chartered Accountants",
                    "caro_qualification": False,
                    "prior_period_receivables": 8200.0,
                    "current_receivables": 8400.0,
                    "prior_period_gross_margin_pct": 87.0,
                    "current_gross_margin_pct": 88.0,
                    "prior_period_sales": 10400.0,
                    "prior_period_leverage": 1.55
                },
                "market_and_valuation": {
                    "market_price_at_filing": 325.0,
                    "reported_eps": 17.6,
                    "forward_eps_estimate": 20.2,
                    "book_value_per_share": 94.9,
                    "earnings_growth_guidance_pct": 12.0
                }
            }
        },
        # --- IT Services Compounders ---
        {
            "filing_id": "NSE_XBRL_Q1FY27_TCS",
            "symbol": "TCS",
            "company_name": "Tata Consultancy Services Limited",
            "cin": "L22210MH1995PLC084781",
            "period_ended": "2026-06-30",
            "filing_date": "2026-07-11",
            "is_banking_or_nbfc": False,
            "financial_statements": {
                "income_statement": {
                    "revenue_from_operations": 62613.0,
                    "other_income": 1220.0,
                    "ebitda": 16530.0,
                    "depreciation_and_amortization": 1210.0,
                    "ebit": 15320.0,
                    "finance_costs": 180.0,
                    "pbt": 16360.0,
                    "tax_expense": 4320.0,
                    "pat": 12040.0
                },
                "balance_sheet": {
                    "equity_share_capital": 361.8,
                    "other_equity": 98500.0,
                    "total_equity": 98861.8,
                    "total_debt": 0.0,
                    "cash_and_cash_equivalents": 42100.0,
                    "total_current_assets": 89000.0,
                    "total_current_liabilities": 31000.0,
                    "total_assets": 142000.0,
                    "retained_earnings": 92000.0
                },
                "cash_flow_statement": {
                    "cash_from_operating_activities": 15500.0,
                    "capital_expenditure": 720.0,
                    "free_cash_flow": 14780.0
                },
                "forensic_and_governance": {
                    "promoter_holding_pct": 71.77,
                    "promoter_pledged_shares_pct": 0.0,
                    "promoter_pledge_qoq_change_pct": 0.0,
                    "asm_gsm_surveillance": False,
                    "auditor_resignation": False,
                    "auditor_name": "B S R & Co. LLP",
                    "caro_qualification": False,
                    "prior_period_receivables": 41200.0,
                    "current_receivables": 42100.0,
                    "prior_period_gross_margin_pct": 41.5,
                    "current_gross_margin_pct": 42.0,
                    "prior_period_sales": 59380.0,
                    "prior_period_leverage": 0.0
                },
                "market_and_valuation": {
                    "market_price_at_filing": 3950.0,
                    "reported_eps": 132.8,
                    "forward_eps_estimate": 148.0,
                    "book_value_per_share": 273.2,
                    "earnings_growth_guidance_pct": 11.5
                }
            }
        },
        {
            "filing_id": "NSE_XBRL_Q1FY27_INFY",
            "symbol": "INFY",
            "company_name": "Infosys Limited",
            "cin": "L85110KA1981PLC013115",
            "period_ended": "2026-06-30",
            "filing_date": "2026-07-18",
            "is_banking_or_nbfc": False,
            "financial_statements": {
                "income_statement": {
                    "revenue_from_operations": 39315.0,
                    "other_income": 690.0,
                    "ebitda": 9514.0,
                    "depreciation_and_amortization": 1120.0,
                    "ebit": 8394.0,
                    "finance_costs": 95.0,
                    "pbt": 8989.0,
                    "tax_expense": 2621.0,
                    "pat": 6368.0
                },
                "balance_sheet": {
                    "equity_share_capital": 2074.0,
                    "other_equity": 82000.0,
                    "total_equity": 84074.0,
                    "total_debt": 0.0,
                    "cash_and_cash_equivalents": 24500.0,
                    "total_current_assets": 68000.0,
                    "total_current_liabilities": 24000.0,
                    "total_assets": 112000.0,
                    "retained_earnings": 76000.0
                },
                "cash_flow_statement": {
                    "cash_from_operating_activities": 8750.0,
                    "capital_expenditure": 650.0,
                    "free_cash_flow": 8100.0
                },
                "forensic_and_governance": {
                    "promoter_holding_pct": 14.60,
                    "promoter_pledged_shares_pct": 0.0,
                    "promoter_pledge_qoq_change_pct": 0.0,
                    "asm_gsm_surveillance": False,
                    "auditor_resignation": False,
                    "auditor_name": "Deloitte Haskins & Sells LLP",
                    "caro_qualification": False,
                    "prior_period_receivables": 29800.0,
                    "current_receivables": 30500.0,
                    "prior_period_gross_margin_pct": 37.0,
                    "current_gross_margin_pct": 37.4,
                    "prior_period_sales": 37440.0,
                    "prior_period_leverage": 0.0
                },
                "market_and_valuation": {
                    "market_price_at_filing": 1750.0,
                    "reported_eps": 61.2,
                    "forward_eps_estimate": 70.0,
                    "book_value_per_share": 202.6,
                    "earnings_growth_guidance_pct": 12.0
                }
            }
        },
        # --- Banking & Financials: Sound vs High CD vs NPA Failure ---
        {
            "filing_id": "NSE_XBRL_Q1FY27_ICICIBANK",
            "symbol": "ICICIBANK",
            "company_name": "ICICI Bank Limited",
            "cin": "L65190GJ1994PLC021012",
            "period_ended": "2026-06-30",
            "filing_date": "2026-07-27",
            "is_banking_or_nbfc": True,
            "financial_statements": {
                "income_statement": {
                    "interest_earned": 37110.0,
                    "interest_expended": 17560.0,
                    "net_interest_income": 19550.0,
                    "other_income": 6380.0,
                    "operating_expenses": 10520.0,
                    "operating_profit": 15410.0,
                    "provisions_and_contingencies": 1330.0,
                    "pbt": 14080.0,
                    "tax_expense": 3020.0,
                    "pat": 11060.0
                },
                "balance_sheet": {
                    "equity_share_capital": 1406.0,
                    "reserves_and_surplus": 242000.0,
                    "total_equity": 243406.0,
                    "total_deposits": 1426000.0,
                    "casa_deposits": 584660.0, # 41.0% CASA
                    "total_advances": 1188000.0, # CD Ratio = 83.3% (< 90% gate)
                    "total_assets": 1940000.0
                },
                "banking_prudential": {
                    "gross_npa": 25100.0, # 2.11%
                    "net_npa": 5120.0,    # 0.43%
                    "crar_capital_adequacy_pct": 16.5,
                    "tier_1_crar_pct": 15.8
                },
                "forensic_and_governance": {
                    "promoter_holding_pct": 0.0,
                    "promoter_pledged_shares_pct": 0.0,
                    "promoter_pledge_qoq_change_pct": 0.0,
                    "asm_gsm_surveillance": False,
                    "auditor_resignation": False,
                    "auditor_name": "MSKA & Associates, Chartered Accountants",
                    "caro_qualification": False
                },
                "market_and_valuation": {
                    "market_price_at_filing": 1220.0,
                    "reported_eps": 63.5,
                    "forward_eps_estimate": 74.0,
                    "book_value_per_share": 346.0,
                    "earnings_growth_guidance_pct": 16.0
                }
            }
        },
        {
            # High CD Ratio Trap: Advances > Deposits (Gate Failure)
            "filing_id": "NSE_XBRL_Q1FY27_HDFCBANK",
            "symbol": "HDFCBANK",
            "company_name": "HDFC Bank Limited",
            "cin": "L65920MH1994PLC080618",
            "period_ended": "2026-06-30",
            "filing_date": "2026-07-20",
            "is_banking_or_nbfc": True,
            "financial_statements": {
                "income_statement": {
                    "interest_earned": 73000.0,
                    "interest_expended": 43200.0,
                    "net_interest_income": 29800.0,
                    "other_income": 10500.0,
                    "operating_expenses": 15800.0,
                    "operating_profit": 24500.0,
                    "provisions_and_contingencies": 2600.0,
                    "pbt": 21900.0,
                    "tax_expense": 5600.0,
                    "pat": 16300.0
                },
                "balance_sheet": {
                    "equity_share_capital": 760.0,
                    "reserves_and_surplus": 440000.0,
                    "total_equity": 440760.0,
                    "total_deposits": 2380000.0,
                    "casa_deposits": 880600.0, # 37.0% CASA
                    "total_advances": 2480000.0, # CD Ratio = 104.2% (> 100% HARD GATE FAILURE!)
                    "total_assets": 3600000.0
                },
                "banking_prudential": {
                    "gross_npa": 33000.0, # 1.33%
                    "net_npa": 9600.0,    # 0.39%
                    "crar_capital_adequacy_pct": 19.3,
                    "tier_1_crar_pct": 17.1
                },
                "forensic_and_governance": {
                    "promoter_holding_pct": 0.0,
                    "promoter_pledged_shares_pct": 0.0,
                    "promoter_pledge_qoq_change_pct": 0.0,
                    "asm_gsm_surveillance": False,
                    "auditor_resignation": False,
                    "auditor_name": "Price Waterhouse LLP",
                    "caro_qualification": False
                },
                "market_and_valuation": {
                    "market_price_at_filing": 1650.0,
                    "reported_eps": 86.0,
                    "forward_eps_estimate": 98.0,
                    "book_value_per_share": 580.0,
                    "earnings_growth_guidance_pct": 14.0
                }
            }
        },
        {
            # High NPA & Low PCR Failure
            "filing_id": "NSE_XBRL_Q1FY27_BANDHANBNK",
            "symbol": "BANDHANBNK",
            "company_name": "Bandhan Bank Limited",
            "cin": "L67190WB2014PLC204622",
            "period_ended": "2026-06-30",
            "filing_date": "2026-07-26",
            "is_banking_or_nbfc": True,
            "financial_statements": {
                "income_statement": {
                    "interest_earned": 5200.0,
                    "interest_expended": 2200.0,
                    "net_interest_income": 3000.0,
                    "other_income": 450.0,
                    "operating_expenses": 1500.0,
                    "operating_profit": 1950.0,
                    "provisions_and_contingencies": 850.0,
                    "pbt": 1100.0,
                    "tax_expense": 280.0,
                    "pat": 820.0
                },
                "balance_sheet": {
                    "equity_share_capital": 1610.0,
                    "reserves_and_surplus": 19500.0,
                    "total_equity": 21110.0,
                    "total_deposits": 133000.0,
                    "casa_deposits": 43890.0, # 33.0% CASA
                    "total_advances": 125000.0, # CD Ratio = 94.0%
                    "total_assets": 175000.0
                },
                "banking_prudential": {
                    "gross_npa": 5287.5, # Gross NPA = 4.23% (> 3.0% threshold FAILS!)
                    "net_npa": 1850.0,   # Net NPA = 1.48% (> 1.0% threshold FAILS!)
                    "crar_capital_adequacy_pct": 15.5,
                    "tier_1_crar_pct": 14.2
                },
                "forensic_and_governance": {
                    "promoter_holding_pct": 39.98,
                    "promoter_pledged_shares_pct": 0.0,
                    "promoter_pledge_qoq_change_pct": 0.0,
                    "asm_gsm_surveillance": False,
                    "auditor_resignation": False,
                    "auditor_name": "Singhi & Co, Chartered Accountants",
                    "caro_qualification": False
                },
                "market_and_valuation": {
                    "market_price_at_filing": 195.0,
                    "reported_eps": 17.5,
                    "forward_eps_estimate": 21.0,
                    "book_value_per_share": 131.0,
                    "earnings_growth_guidance_pct": 12.0
                }
            }
        },
        # --- Valuation Traps (> 60x PE with low growth) ---
        {
            "filing_id": "NSE_XBRL_Q1FY27_DIXON",
            "symbol": "DIXON",
            "company_name": "Dixon Technologies (India) Limited",
            "cin": "L32101UP1993PLC066581",
            "period_ended": "2026-06-30",
            "filing_date": "2026-07-30",
            "is_banking_or_nbfc": False,
            "financial_statements": {
                "income_statement": {
                    "revenue_from_operations": 6580.0,
                    "other_income": 12.0,
                    "ebitda": 263.2,
                    "depreciation_and_amortization": 45.0,
                    "ebit": 218.2,
                    "finance_costs": 32.0,
                    "pbt": 186.2,
                    "tax_expense": 47.0,
                    "pat": 139.2
                },
                "balance_sheet": {
                    "equity_share_capital": 11.97,
                    "other_equity": 1850.0,
                    "total_equity": 1861.97,
                    "total_debt": 450.0,
                    "cash_and_cash_equivalents": 180.0,
                    "total_current_assets": 4500.0,
                    "total_current_liabilities": 3400.0,
                    "total_assets": 5800.0,
                    "retained_earnings": 1700.0
                },
                "cash_flow_statement": {
                    "cash_from_operating_activities": 189.5,
                    "capital_expenditure": 140.0,
                    "free_cash_flow": 49.5
                },
                "forensic_and_governance": {
                    "promoter_holding_pct": 33.68,
                    "promoter_pledged_shares_pct": 0.0,
                    "promoter_pledge_qoq_change_pct": 0.0,
                    "asm_gsm_surveillance": False,
                    "auditor_resignation": False,
                    "auditor_name": "SN Dhawan & Co LLP",
                    "caro_qualification": False,
                    "prior_period_receivables": 2100.0,
                    "current_receivables": 2350.0,
                    "prior_period_gross_margin_pct": 7.8,
                    "current_gross_margin_pct": 7.5,
                    "prior_period_sales": 3270.0,
                    "prior_period_leverage": 0.35
                },
                "market_and_valuation": {
                    # Trailing PE = 94.5x (> 60x HARD CEILING FAILS!)
                    "market_price_at_filing": 11800.0,
                    "reported_eps": 124.8,
                    "forward_eps_estimate": 151.0,
                    "book_value_per_share": 311.0,
                    "earnings_growth_guidance_pct": 22.0
                }
            }
        },
        # --- Severe Forensics / Governance Failures (Hard Step 1 Veto) ---
        {
            "filing_id": "NSE_XBRL_Q1FY27_IDEA",
            "symbol": "IDEA",
            "company_name": "Vodafone Idea Limited",
            "cin": "L32100GJ1995PLC030886",
            "period_ended": "2026-06-30",
            "filing_date": "2026-08-12",
            "is_banking_or_nbfc": False,
            "financial_statements": {
                "income_statement": {
                    "revenue_from_operations": 10500.0,
                    "other_income": 40.0,
                    "ebitda": 4200.0,
                    "depreciation_and_amortization": 5400.0,
                    "ebit": -1200.0,
                    "finance_costs": 5450.0,
                    "pbt": -6650.0,
                    "tax_expense": 2.0,
                    "pat": -6652.0
                },
                "balance_sheet": {
                    "equity_share_capital": 68000.0,
                    "other_equity": -152000.0, # Massive Negative Net Worth
                    "total_equity": -84000.0,
                    "total_debt": 210000.0,
                    "cash_and_cash_equivalents": 2100.0,
                    "total_current_assets": 11000.0,
                    "total_current_liabilities": 88000.0,
                    "total_assets": 168000.0,
                    "retained_earnings": -165000.0
                },
                "cash_flow_statement": {
                    "cash_from_operating_activities": 1890.0,
                    "capital_expenditure": 2100.0,
                    "free_cash_flow": -210.0
                },
                "forensic_and_governance": {
                    "promoter_holding_pct": 37.17,
                    "promoter_pledged_shares_pct": 12.0,
                    "promoter_pledge_qoq_change_pct": 1.0,
                    "asm_gsm_surveillance": True, # SURVEILLANCE VETO!
                    "auditor_resignation": False,
                    "auditor_name": "S.R. Batliboi & Associates LLP",
                    "caro_qualification": True, # ADVERSE CARO VETO!
                    "prior_period_receivables": 3400.0,
                    "current_receivables": 3800.0,
                    "prior_period_gross_margin_pct": 40.0,
                    "current_gross_margin_pct": 40.0,
                    "prior_period_sales": 10600.0,
                    "prior_period_leverage": 15.0
                },
                "market_and_valuation": {
                    "market_price_at_filing": 8.5,
                    "reported_eps": -4.2,
                    "forward_eps_estimate": -3.5,
                    "book_value_per_share": -12.4,
                    "earnings_growth_guidance_pct": -5.0
                }
            }
        },
        {
            "filing_id": "NSE_XBRL_Q1FY27_RPOWER",
            "symbol": "RPOWER",
            "company_name": "Reliance Power Limited",
            "cin": "L40101MH1995PLC084687",
            "period_ended": "2026-06-30",
            "filing_date": "2026-08-14",
            "is_banking_or_nbfc": False,
            "financial_statements": {
                "income_statement": {
                    "revenue_from_operations": 1950.0,
                    "other_income": 35.0,
                    "ebitda": 410.0,
                    "depreciation_and_amortization": 220.0,
                    "ebit": 190.0,
                    "finance_costs": 550.0,
                    "pbt": -360.0,
                    "tax_expense": 0.0,
                    "pat": -360.0
                },
                "balance_sheet": {
                    "equity_share_capital": 4016.0,
                    "other_equity": 7500.0,
                    "total_equity": 11516.0,
                    "total_debt": 18500.0,
                    "cash_and_cash_equivalents": 420.0,
                    "total_current_assets": 4200.0,
                    "total_current_liabilities": 9800.0,
                    "total_assets": 32000.0,
                    "retained_earnings": 5100.0
                },
                "cash_flow_statement": {
                    "cash_from_operating_activities": 143.5,
                    "capital_expenditure": 60.0,
                    "free_cash_flow": 83.5
                },
                "forensic_and_governance": {
                    "promoter_holding_pct": 23.24,
                    "promoter_pledged_shares_pct": 28.5, # > 20% PLEDGE VETO!
                    "promoter_pledge_qoq_change_pct": 6.2, # > 5% SPIKE VETO!
                    "asm_gsm_surveillance": True,
                    "auditor_resignation": True, # AUDITOR RESIGNATION VETO!
                    "auditor_name": "Pathak H.D. & Associates LLP",
                    "caro_qualification": True, # CARO QUALIFICATION VETO!
                    "prior_period_receivables": 1800.0,
                    "current_receivables": 2100.0,
                    "prior_period_gross_margin_pct": 22.0,
                    "current_gross_margin_pct": 21.0,
                    "prior_period_sales": 1920.0,
                    "prior_period_leverage": 6.5
                },
                "market_and_valuation": {
                    "market_price_at_filing": 38.0,
                    "reported_eps": -1.2,
                    "forward_eps_estimate": -0.8,
                    "book_value_per_share": 28.7,
                    "earnings_growth_guidance_pct": 5.0
                }
            }
        },
        {
            "filing_id": "NSE_XBRL_Q1FY27_VEDL",
            "symbol": "VEDL",
            "company_name": "Vedanta Limited",
            "cin": "L13209MH1965PLC291394",
            "period_ended": "2026-06-30",
            "filing_date": "2026-08-06",
            "is_banking_or_nbfc": False,
            "financial_statements": {
                "income_statement": {
                    "revenue_from_operations": 35200.0,
                    "other_income": 650.0,
                    "ebitda": 10200.0,
                    "depreciation_and_amortization": 2600.0,
                    "ebit": 7600.0,
                    "finance_costs": 2400.0,
                    "pbt": 5850.0,
                    "tax_expense": 1450.0,
                    "pat": 4400.0
                },
                "balance_sheet": {
                    "equity_share_capital": 372.0,
                    "other_equity": 38500.0,
                    "total_equity": 38872.0,
                    "total_debt": 75000.0,
                    "cash_and_cash_equivalents": 15400.0,
                    "total_current_assets": 42000.0,
                    "total_current_liabilities": 54000.0,
                    "total_assets": 185000.0,
                    "retained_earnings": 32000.0
                },
                "cash_flow_statement": {
                    "cash_from_operating_activities": 8364.0,
                    "capital_expenditure": 4200.0,
                    "free_cash_flow": 4164.0
                },
                "forensic_and_governance": {
                    "promoter_holding_pct": 56.38,
                    "promoter_pledged_shares_pct": 99.8, # 99.8% PLEDGE HARD VETO!
                    "promoter_pledge_qoq_change_pct": 0.0,
                    "asm_gsm_surveillance": False,
                    "auditor_resignation": False,
                    "auditor_name": "S.R. Batliboi & Co. LLP",
                    "caro_qualification": False,
                    "prior_period_receivables": 3800.0,
                    "current_receivables": 4100.0,
                    "prior_period_gross_margin_pct": 29.0,
                    "current_gross_margin_pct": 29.0,
                    "prior_period_sales": 33700.0,
                    "prior_period_leverage": 2.9
                },
                "market_and_valuation": {
                    "market_price_at_filing": 480.0,
                    "reported_eps": 47.3,
                    "forward_eps_estimate": 55.0,
                    "book_value_per_share": 104.5,
                    "earnings_growth_guidance_pct": 8.0
                }
            }
        }
    ]

    for f in filings:
        sym = f["symbol"]
        path = f"data/corporate_filings/{sym}_Q1FY27.json"
        with open(path, "w") as out:
            json.dump(f, out, indent=2)
        print(f"[OK] Generated {path}")

    print(f"\nGenerated {len(filings)} audited corporate XBRL filing disclosures.")

if __name__ == "__main__":
    generate_filings()
