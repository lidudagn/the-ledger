"""
Seed the applicant_registry schema from seed_events.jsonl.

Reads the event payloads to extract company IDs and financial data,
then generates the read-only CRM tables that agents query.
"""
import asyncio
import json
import hashlib
import random
from datetime import date
from pathlib import Path
from decimal import Decimal

import asyncpg


SEED_FILE = Path(__file__).parent.parent / "seed_events.jsonl"
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# ── Schema DDL ──────────────────────────────────────────────────

SCHEMA_DDL = """
CREATE SCHEMA IF NOT EXISTS applicant_registry;

CREATE TABLE IF NOT EXISTS applicant_registry.companies (
    company_id       TEXT PRIMARY KEY,
    company_name     TEXT NOT NULL,
    legal_type       TEXT NOT NULL,
    sector           TEXT NOT NULL,
    jurisdiction     TEXT NOT NULL,
    founded_year     INTEGER NOT NULL,
    employee_count   INTEGER,
    annual_revenue   NUMERIC,
    trajectory       TEXT NOT NULL,
    risk_tier        TEXT NOT NULL,
    ein              TEXT,
    address          TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS applicant_registry.financial_history (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    company_id       TEXT NOT NULL REFERENCES applicant_registry.companies(company_id),
    fiscal_year      INTEGER NOT NULL,
    total_revenue    NUMERIC NOT NULL,
    cost_of_goods    NUMERIC,
    gross_profit     NUMERIC,
    ebitda           NUMERIC,
    net_income       NUMERIC NOT NULL,
    total_assets     NUMERIC NOT NULL,
    total_liabilities NUMERIC NOT NULL,
    total_equity     NUMERIC,
    operating_margin NUMERIC,
    debt_to_equity   NUMERIC,
    current_ratio    NUMERIC,
    return_on_assets NUMERIC,
    CONSTRAINT uq_company_fiscal_year UNIQUE (company_id, fiscal_year)
);

CREATE TABLE IF NOT EXISTS applicant_registry.compliance_flags (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    company_id       TEXT NOT NULL REFERENCES applicant_registry.companies(company_id),
    flag_type        TEXT NOT NULL,
    severity         TEXT NOT NULL DEFAULT 'MEDIUM',
    is_active        BOOLEAN NOT NULL DEFAULT TRUE,
    flagged_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at      TIMESTAMPTZ,
    notes            TEXT
);

CREATE TABLE IF NOT EXISTS applicant_registry.loan_relationships (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    company_id       TEXT NOT NULL REFERENCES applicant_registry.companies(company_id),
    loan_id          TEXT NOT NULL,
    loan_amount_usd  NUMERIC NOT NULL,
    loan_date        DATE NOT NULL,
    maturity_date    DATE,
    status           TEXT NOT NULL,
    default_occurred BOOLEAN NOT NULL DEFAULT FALSE,
    relationship_years INTEGER,
    notes            TEXT
);

CREATE INDEX IF NOT EXISTS idx_fin_hist_company
    ON applicant_registry.financial_history (company_id, fiscal_year);
CREATE INDEX IF NOT EXISTS idx_compliance_flags_company
    ON applicant_registry.compliance_flags (company_id, is_active);
CREATE INDEX IF NOT EXISTS idx_loan_rel_company
    ON applicant_registry.loan_relationships (company_id);
"""


# ── Deterministic company profiles ──────────────────────────────
# We derive properties from the company ID number for reproducibility.

SECTORS = [
    "technology", "healthcare", "manufacturing", "retail", "energy",
    "agriculture", "logistics", "fintech", "construction", "food",
    "aerospace", "media", "mining", "real_estate", "hospitality",
    "insurance", "automotive", "chemicals", "telecom", "services",
]
TRAJECTORIES = ["GROWTH", "STABLE", "DECLINING", "RECOVERING", "VOLATILE"]
RISK_TIERS = ["LOW", "MEDIUM", "HIGH"]
LEGAL_TYPES = ["LLC", "Corporation", "Partnership", "Sole Proprietor"]
JURISDICTIONS = [
    "CA", "NY", "TX", "FL", "IL", "PA", "OH", "MI", "GA", "WA",
    "MA", "NJ", "VA", "NC", "AZ", "CO", "MN", "WI", "OR", "CT",
    "IN", "TN", "MO", "MD", "SC", "AL", "LA", "KY", "OK", "IA",
    "KS", "NV", "NE", "WV", "HI", "ME", "ND", "MT",  # MT at index 37
]


def _seed(company_id: str) -> int:
    """Deterministic seed from company ID."""
    num = int(company_id.replace("COMP-", ""))
    return num


def generate_company_profile(company_id: str, event_data: dict) -> dict:
    """Generate a deterministic company profile from company_id and event hints."""
    s = _seed(company_id)
    rng = random.Random(s)

    sector = SECTORS[s % len(SECTORS)]
    trajectory = event_data.get("trajectory", TRAJECTORIES[s % len(TRAJECTORIES)])
    risk_tier = event_data.get("risk_tier", RISK_TIERS[s % len(RISK_TIERS)])

    # Use event-derived revenue if available, otherwise generate
    revenue = event_data.get("revenue")
    if revenue is None:
        revenue = rng.randint(800_000, 20_000_000)

    return {
        "company_id": company_id,
        "company_name": f"Company {company_id}",
        "legal_type": LEGAL_TYPES[s % len(LEGAL_TYPES)],
        "sector": sector,
        "jurisdiction": JURISDICTIONS[s % len(JURISDICTIONS)],
        "founded_year": rng.randint(1998, 2021),
        "employee_count": rng.randint(10, 300),
        "annual_revenue": revenue,
        "trajectory": trajectory,
        "risk_tier": risk_tier,
        "ein": f"{s:02d}-{rng.randint(1000000, 9999999)}",
        "address": f"{rng.randint(100, 999)} Main St, USA",
    }


def generate_financial_history(company_id: str, base_revenue: float, trajectory: str) -> list[dict]:
    """Generate 3 years of GAAP financials based on trajectory."""
    s = _seed(company_id)
    rng = random.Random(s + 1000)

    # Revenue growth rates by trajectory
    growth_map = {
        "GROWTH": (0.10, 0.18),
        "STABLE": (-0.02, 0.04),
        "DECLINING": (-0.12, -0.04),
        "RECOVERING": (0.02, 0.10),
        "VOLATILE": (-0.15, 0.20),
    }
    lo, hi = growth_map.get(trajectory, (-0.02, 0.04))

    rows = []
    rev = base_revenue
    for year in [2022, 2023, 2024]:
        rate = rng.uniform(lo, hi)
        if year > 2022:
            rev = rev * (1 + rate)

        cogs = rev * rng.uniform(0.45, 0.75)
        gp = rev - cogs
        ebitda = gp * rng.uniform(0.40, 0.70)
        ni = ebitda * rng.uniform(0.30, 0.60)
        assets = rev * rng.uniform(1.0, 1.8)
        liab = assets * rng.uniform(0.35, 0.70)
        equity = assets - liab

        rows.append({
            "company_id": company_id,
            "fiscal_year": year,
            "total_revenue": round(rev, 2),
            "cost_of_goods": round(cogs, 2),
            "gross_profit": round(gp, 2),
            "ebitda": round(ebitda, 2),
            "net_income": round(ni, 2),
            "total_assets": round(assets, 2),
            "total_liabilities": round(liab, 2),
            "total_equity": round(equity, 2),
            "operating_margin": round(ni / rev, 4) if rev else 0,
            "debt_to_equity": round(liab / equity, 4) if equity > 0 else None,
            "current_ratio": round(rng.uniform(0.8, 3.0), 2),
            "return_on_assets": round(ni / assets, 4) if assets else 0,
        })
    return rows


def extract_company_hints(events: list[dict]) -> dict[str, dict]:
    """Extract company data hints from event payloads."""
    hints: dict[str, dict] = {}

    for e in events:
        p = e.get("payload", {})
        etype = e["event_type"]

        # Get applicant_id from ApplicationSubmitted
        if etype == "ApplicationSubmitted" and "applicant_id" in p:
            cid = p["applicant_id"]
            if cid not in hints:
                hints[cid] = {}

        # Get trajectory/revenue hints from HistoricalProfileConsumed
        if etype == "HistoricalProfileConsumed":
            app_id = p.get("application_id", "")
            traj = p.get("revenue_trajectory")
            # Find company for this application
            for e2 in events:
                if (e2["event_type"] == "ApplicationSubmitted"
                        and e2["payload"].get("application_id") == app_id):
                    cid = e2["payload"]["applicant_id"]
                    if cid not in hints:
                        hints[cid] = {}
                    if traj:
                        hints[cid]["trajectory"] = traj
                    hints[cid]["has_prior_loans"] = p.get("has_prior_loans", False)
                    hints[cid]["has_defaults"] = p.get("has_defaults", False)
                    break

        # Get revenue from ExtractedFactsConsumed
        if etype == "ExtractedFactsConsumed":
            summary = p.get("facts_summary", "")
            if "Revenue $" in summary:
                try:
                    rev_str = summary.split("Revenue $")[1].split(",")[0].replace(",", "")
                    rev = float(rev_str)
                    app_id = p.get("application_id", "")
                    for e2 in events:
                        if (e2["event_type"] == "ApplicationSubmitted"
                                and e2["payload"].get("application_id") == app_id):
                            cid = e2["payload"]["applicant_id"]
                            if cid not in hints:
                                hints[cid] = {}
                            hints[cid]["revenue"] = rev
                            break
                except (ValueError, IndexError):
                    pass

        # Detect compliance failures
        if etype == "ComplianceRuleFailed":
            rule_id = p.get("rule_id", "")
            app_id = p.get("application_id", "")
            for e2 in events:
                if (e2["event_type"] == "ApplicationSubmitted"
                        and e2["payload"].get("application_id") == app_id):
                    cid = e2["payload"]["applicant_id"]
                    if cid not in hints:
                        hints[cid] = {}
                    hints[cid].setdefault("failed_rules", []).append(rule_id)
                    break

    return hints


async def seed_registry(db_url: str = DATABASE_URL):
    """Main seeding function."""
    # Load events
    events = []
    with open(SEED_FILE) as f:
        for line in f:
            events.append(json.loads(line.strip()))

    print(f"Loaded {len(events)} events from {SEED_FILE}")

    # Extract company hints from events
    hints = extract_company_hints(events)

    # Also find ALL unique company IDs (some may not have hints)
    all_company_ids = set()
    for e in events:
        cid = e.get("payload", {}).get("applicant_id")
        if cid and cid.startswith("COMP-"):
            all_company_ids.add(cid)
    for cid in hints:
        all_company_ids.add(cid)

    print(f"Found {len(all_company_ids)} unique companies")

    # Generate 80 companies (fill in unreferenced ones)
    for i in range(1, 81):
        cid = f"COMP-{i:03d}"
        if cid not in all_company_ids:
            all_company_ids.add(cid)

    conn = await asyncpg.connect(db_url)
    try:
        # Create schema
        await conn.execute(SCHEMA_DDL)
        print("Schema created")

        # Insert companies
        companies_inserted = 0
        for cid in sorted(all_company_ids):
            h = hints.get(cid, {})
            profile = generate_company_profile(cid, h)

            # Handle special cases from events
            failed_rules = h.get("failed_rules", [])
            if "REG-002" in failed_rules:
                # OFAC failure → needs SANCTIONS_REVIEW flag
                pass  # handled below in flags
            if "REG-003" in failed_rules:
                profile["jurisdiction"] = "MT"  # Montana hard block

            await conn.execute("""
                INSERT INTO applicant_registry.companies
                    (company_id, company_name, legal_type, sector, jurisdiction,
                     founded_year, employee_count, annual_revenue, trajectory,
                     risk_tier, ein, address)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                ON CONFLICT (company_id) DO NOTHING
            """, profile["company_id"], profile["company_name"],
                profile["legal_type"], profile["sector"], profile["jurisdiction"],
                profile["founded_year"], profile["employee_count"],
                Decimal(str(profile["annual_revenue"])),
                profile["trajectory"], profile["risk_tier"],
                profile["ein"], profile["address"])
            companies_inserted += 1

        print(f"Inserted {companies_inserted} companies")

        # Insert financial history (3 years per company)
        fin_inserted = 0
        for cid in sorted(all_company_ids):
            h = hints.get(cid, {})
            profile = generate_company_profile(cid, h)
            rows = generate_financial_history(
                cid,
                profile["annual_revenue"],
                profile["trajectory"],
            )
            for row in rows:
                await conn.execute("""
                    INSERT INTO applicant_registry.financial_history
                        (company_id, fiscal_year, total_revenue, cost_of_goods,
                         gross_profit, ebitda, net_income, total_assets,
                         total_liabilities, total_equity, operating_margin,
                         debt_to_equity, current_ratio, return_on_assets)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
                    ON CONFLICT (company_id, fiscal_year) DO NOTHING
                """, row["company_id"], row["fiscal_year"],
                    Decimal(str(row["total_revenue"])),
                    Decimal(str(row["cost_of_goods"])),
                    Decimal(str(row["gross_profit"])),
                    Decimal(str(row["ebitda"])),
                    Decimal(str(row["net_income"])),
                    Decimal(str(row["total_assets"])),
                    Decimal(str(row["total_liabilities"])),
                    Decimal(str(row["total_equity"])),
                    Decimal(str(row["operating_margin"])),
                    Decimal(str(row["debt_to_equity"])) if row["debt_to_equity"] else None,
                    Decimal(str(row["current_ratio"])),
                    Decimal(str(row["return_on_assets"])))
                fin_inserted += 1

        print(f"Inserted {fin_inserted} financial history rows")

        # Insert compliance flags (derived from event failures)
        flags_inserted = 0
        for cid, h in hints.items():
            failed_rules = h.get("failed_rules", [])
            if "REG-001" in failed_rules:
                await conn.execute("""
                    INSERT INTO applicant_registry.compliance_flags
                        (company_id, flag_type, severity, is_active, notes)
                    VALUES ($1, 'AML_WATCH', 'HIGH', TRUE, 'BSA compliance flag')
                    ON CONFLICT DO NOTHING
                """, cid)
                flags_inserted += 1
            if "REG-002" in failed_rules:
                await conn.execute("""
                    INSERT INTO applicant_registry.compliance_flags
                        (company_id, flag_type, severity, is_active, notes)
                    VALUES ($1, 'SANCTIONS_REVIEW', 'HIGH', TRUE, 'OFAC sanctions flag')
                    ON CONFLICT DO NOTHING
                """, cid)
                flags_inserted += 1

        print(f"Inserted {flags_inserted} compliance flags")

        # Insert loan relationships (derived from HistoricalProfileConsumed hints)
        loans_inserted = 0
        for cid, h in hints.items():
            if h.get("has_prior_loans"):
                s = _seed(cid)
                rng = random.Random(s + 2000)
                num_loans = rng.randint(1, 3)
                for i in range(num_loans):
                    default = h.get("has_defaults", False) and i == 0
                    loan_yr = rng.randint(2018, 2024)
                    loan_mo = rng.randint(1, 12)
                    loan_dy = rng.randint(1, 28)
                    mat_yr = rng.randint(2025, 2030)
                    mat_mo = rng.randint(1, 12)
                    mat_dy = rng.randint(1, 28)
                    await conn.execute("""
                        INSERT INTO applicant_registry.loan_relationships
                            (company_id, loan_id, loan_amount_usd, loan_date,
                             maturity_date, status, default_occurred,
                             relationship_years, notes)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        ON CONFLICT DO NOTHING
                    """, cid,
                        f"LOAN-{cid}-{i+1:03d}",
                        Decimal(str(rng.randint(100_000, 5_000_000))),
                        date(loan_yr, loan_mo, loan_dy),
                        date(mat_yr, mat_mo, mat_dy),
                        "DEFAULT" if default else rng.choice(["ACTIVE", "REPAID"]),
                        default,
                        rng.randint(2, 15),
                        "Prior loan relationship")
                    loans_inserted += 1

        print(f"Inserted {loans_inserted} loan relationships")

        # Verification
        counts = await conn.fetch("""
            SELECT 'companies' as tbl, count(*) as cnt FROM applicant_registry.companies
            UNION ALL
            SELECT 'financial_history', count(*) FROM applicant_registry.financial_history
            UNION ALL
            SELECT 'compliance_flags', count(*) FROM applicant_registry.compliance_flags
            UNION ALL
            SELECT 'loan_relationships', count(*) FROM applicant_registry.loan_relationships
        """)
        print("\n=== VERIFICATION ===")
        for row in counts:
            print(f"  {row['tbl']}: {row['cnt']}")

        traj = await conn.fetch("""
            SELECT trajectory, count(*) as cnt
            FROM applicant_registry.companies
            GROUP BY trajectory ORDER BY trajectory
        """)
        print("\n=== TRAJECTORY DISTRIBUTION ===")
        for row in traj:
            print(f"  {row['trajectory']}: {row['cnt']}")

        risk = await conn.fetch("""
            SELECT risk_tier, count(*) as cnt
            FROM applicant_registry.companies
            GROUP BY risk_tier ORDER BY risk_tier
        """)
        print("\n=== RISK TIER DISTRIBUTION ===")
        for row in risk:
            print(f"  {row['risk_tier']}: {row['cnt']}")

        print("\nSEED COMPLETE ✅")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(seed_registry())
