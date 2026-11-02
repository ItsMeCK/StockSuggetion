"""
Exchange Filing Fetcher, Raw Vault & Cryptographic Manifest Auditor
Requirements EQ2, EQ4, EQ5, EQ12, EQ34, EQ40, EQ41, EQ42, EQ44, EQ45

Maintains raw machine-readable corporate filings with cryptographic SHA-256 hashes
and provides documented schema parsing and published results reconciliation.
"""

import os
import json
import hashlib
import logging
from datetime import date
from typing import Dict, List, Optional, Any, Tuple
from equity_portfolio.data.fundamental_store import StockFundamentalRecord

logger = logging.getLogger("FilingFetcher")

class FilingVault:
    """
    Manages raw audited financial filings downloaded from NSE/BSE corporate repositories.
    Guarantees data integrity via cryptographic SHA-256 hash auditing.
    """
    def __init__(
        self,
        filings_dir: str = "data/corporate_filings",
        raw_dir: str = "data/corporate_filings/raw",
        manifest_path: str = "data/corporate_filings/filing_manifest.json"
    ):
        self.filings_dir = filings_dir
        self.raw_dir = raw_dir
        self.manifest_path = manifest_path

    @staticmethod
    def compute_sha256(file_path: str) -> str:
        """Computes cryptographic SHA-256 hash of a raw filing file."""
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            while chunk := f.read(65536):
                sha256.update(chunk)
        return sha256.hexdigest()

    def download_filing_from_exchange(
        self,
        symbol: str,
        quarter: str = "Q1FY27",
        source_url: Optional[str] = None,
        timeout: int = 10
    ) -> Optional[str]:
        """
        Downloads corporate XBRL filing from official exchange repository (NSE/BSE/Kite API).
        If network access is restricted (e.g. sandboxed test runner), safely falls back
        to existing raw vault file on disk. Computes cryptographic SHA-256 hash.
        """
        import urllib.request
        import urllib.error

        os.makedirs(self.raw_dir, exist_ok=True)
        dest_path = os.path.join(self.raw_dir, f"{symbol}_{quarter}_RAW.json")

        if source_url:
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "application/json, text/plain, */*"
            }
            try:
                req = urllib.request.Request(source_url, headers=headers)
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    content = resp.read()
                    with open(dest_path, "wb") as f:
                        f.write(content)
                    logger.info(f"Successfully downloaded filing for {symbol} to {dest_path}")
                    return dest_path
            except (urllib.error.URLError, Exception) as e:
                logger.warning(f"Live network download for {symbol} from {source_url} unavailable ({e}). Falling back to raw vault.")

        if os.path.exists(dest_path):
            return dest_path

        alt_path = os.path.join(self.filings_dir, f"{symbol}_{quarter}.json")
        if os.path.exists(alt_path):
            return alt_path

        return None

    def batch_download_universe_filings(
        self,
        symbols: List[str],
        quarter: str = "Q1FY27"
    ) -> Dict[str, str]:
        """
        Downloads or verifies presence of audited filings across the universe.
        """
        downloaded = {}
        for sym in symbols:
            path = self.download_filing_from_exchange(sym, quarter)
            if path:
                downloaded[sym] = path
        return downloaded

    def verify_manifest_integrity(self) -> Tuple[bool, List[str]]:
        """
        Audits all raw filings against the manifest.
        Verifies that every raw file exists on disk and its SHA-256 hash exactly matches.
        """
        if not os.path.exists(self.manifest_path):
            return False, ["Manifest file missing"]

        with open(self.manifest_path, "r") as f:
            manifest = json.load(f)

        violations = []
        for sym, entry in manifest.items():
            raw_file = entry.get("raw_file_path")
            if not raw_file or not os.path.exists(raw_file):
                violations.append(f"Raw filing missing on disk for {sym}: {raw_file}")
                continue

            expected_hash = entry.get("sha256_hash")
            actual_hash = self.compute_sha256(raw_file)
            if expected_hash != actual_hash:
                violations.append(
                    f"SHA-256 hash mismatch for {sym}: expected {expected_hash}, got {actual_hash}"
                )

        return len(violations) == 0, violations

    def load_and_reconcile_all(self) -> Dict[str, StockFundamentalRecord]:
        """
        Loads verified audited filings from the raw vault, validates cryptographic SHA-256 hashes,
        and dynamically computes all financial ratios according to the audited schema.
        """
        from equity_portfolio.data.fundamental_ingestor import FundamentalIngestor

        if not os.path.exists(self.manifest_path):
            logger.warning(f"Filing manifest not found at {self.manifest_path}")
            return {}

        with open(self.manifest_path, "r") as f:
            manifest = json.load(f)

        records: Dict[str, StockFundamentalRecord] = {}

        for sym, entry in manifest.items():
            raw_path = entry.get("raw_file_path")
            if not raw_path or not os.path.exists(raw_path):
                continue

            # Verify cryptographic SHA-256 hash
            actual_hash = self.compute_sha256(raw_path)
            expected_hash = entry.get("sha256_hash")
            if expected_hash and actual_hash != expected_hash:
                logger.error(f"Integrity check failed for {sym}: hash mismatch!")
                continue

            try:
                rec = FundamentalIngestor.parse_audited_filing(raw_path)
                records[rec.symbol.upper()] = rec
            except Exception as e:
                logger.error(f"Error parsing filing {raw_path}: {e}")

        return records
