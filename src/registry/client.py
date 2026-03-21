"""
ApplicantRegistryClient — Read-only async client for the Applicant Registry CRM.

Pure asyncpg. No ORM. Every query returns Pydantic models.
This client is the ONLY interface agents use to access the external CRM.
The event store NEVER writes to the applicant_registry schema.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import asyncpg


# ── Data Models ──────────────────────────────────────────────────


@dataclass(frozen=True)
class CompanyProfile:
    """Immutable company profile from the CRM."""
    company_id: str
    company_name: str
    legal_type: str
    sector: str
    jurisdiction: str
    founded_year: int
    employee_count: Optional[int]
    annual_revenue: Optional[Decimal]
    trajectory: str
    risk_tier: str
    ein: Optional[str]
    address: Optional[str]


@dataclass(frozen=True)
class FinancialYear:
    """Single fiscal year of GAAP financial data."""
    company_id: str
    fiscal_year: int
    total_revenue: Decimal
    cost_of_goods: Optional[Decimal]
    gross_profit: Optional[Decimal]
    ebitda: Optional[Decimal]
    net_income: Decimal
    total_assets: Decimal
    total_liabilities: Decimal
    total_equity: Optional[Decimal]
    operating_margin: Optional[Decimal]
    debt_to_equity: Optional[Decimal]
    current_ratio: Optional[Decimal]
    return_on_assets: Optional[Decimal]


@dataclass(frozen=True)
class ComplianceFlag:
    """Active or historical compliance flag."""
    company_id: str
    flag_type: str
    severity: str
    is_active: bool
    flagged_at: datetime
    resolved_at: Optional[datetime]
    notes: Optional[str]


@dataclass(frozen=True)
class LoanRelationship:
    """Historical loan record for a company."""
    company_id: str
    loan_id: str
    loan_amount_usd: Decimal
    loan_date: date
    maturity_date: Optional[date]
    status: str
    default_occurred: bool
    relationship_years: Optional[int]
    notes: Optional[str]


# ── Client ───────────────────────────────────────────────────────


class ApplicantRegistryClient:
    """
    Async read-only client for the applicant_registry schema.

    Usage:
        pool = await asyncpg.create_pool(DATABASE_URL)
        client = ApplicantRegistryClient(pool)
        company = await client.get_company("COMP-031")
    """

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    # ── Company Profile ──────────────────────────────────────────

    async def get_company(self, company_id: str) -> Optional[CompanyProfile]:
        """
        Fetch a single company profile.
        Returns None if the company does not exist.
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT company_id, company_name, legal_type, sector, jurisdiction,
                       founded_year, employee_count, annual_revenue, trajectory,
                       risk_tier, ein, address
                FROM applicant_registry.companies
                WHERE company_id = $1
                """,
                company_id,
            )
        if row is None:
            return None
        return CompanyProfile(**dict(row))

    # ── Financial History ────────────────────────────────────────

    async def get_financial_history(
        self, company_id: str
    ) -> list[FinancialYear]:
        """
        Fetch all fiscal years of financial data for a company.
        Returns empty list if no history exists.
        Ordered by fiscal_year ascending (oldest first).
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT company_id, fiscal_year, total_revenue, cost_of_goods,
                       gross_profit, ebitda, net_income, total_assets,
                       total_liabilities, total_equity, operating_margin,
                       debt_to_equity, current_ratio, return_on_assets
                FROM applicant_registry.financial_history
                WHERE company_id = $1
                ORDER BY fiscal_year ASC
                """,
                company_id,
            )
        return [FinancialYear(**dict(r)) for r in rows]

    # ── Compliance Flags ─────────────────────────────────────────

    async def get_compliance_flags(
        self,
        company_id: str,
        *,
        active_only: bool = False,
    ) -> list[ComplianceFlag]:
        """
        Fetch compliance flags for a company.
        If active_only=True, returns only flags with is_active=TRUE.
        Returns empty list if no flags exist.
        """
        if active_only:
            query = """
                SELECT company_id, flag_type, severity, is_active,
                       flagged_at, resolved_at, notes
                FROM applicant_registry.compliance_flags
                WHERE company_id = $1 AND is_active = TRUE
                ORDER BY flagged_at DESC
            """
        else:
            query = """
                SELECT company_id, flag_type, severity, is_active,
                       flagged_at, resolved_at, notes
                FROM applicant_registry.compliance_flags
                WHERE company_id = $1
                ORDER BY flagged_at DESC
            """

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, company_id)
        return [ComplianceFlag(**dict(r)) for r in rows]

    # ── Loan Relationships ───────────────────────────────────────

    async def get_loan_relationships(
        self, company_id: str
    ) -> list[LoanRelationship]:
        """
        Fetch all loan relationships for a company.
        Returns empty list if no relationships exist.
        Ordered by loan_date descending (most recent first).
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT company_id, loan_id, loan_amount_usd, loan_date,
                       maturity_date, status, default_occurred,
                       relationship_years, notes
                FROM applicant_registry.loan_relationships
                WHERE company_id = $1
                ORDER BY loan_date DESC
                """,
                company_id,
            )
        return [LoanRelationship(**dict(r)) for r in rows]

    # ── Convenience: Has Defaults? ───────────────────────────────

    async def has_prior_defaults(self, company_id: str) -> bool:
        """Check if a company has any loan with default_occurred=TRUE."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT EXISTS(
                    SELECT 1 FROM applicant_registry.loan_relationships
                    WHERE company_id = $1 AND default_occurred = TRUE
                ) AS has_default
                """,
                company_id,
            )
        return row["has_default"] if row else False

    # ── Convenience: Has Active High Compliance Flag? ────────────

    async def has_active_high_flag(self, company_id: str) -> bool:
        """Check if a company has any active HIGH-severity compliance flag."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT EXISTS(
                    SELECT 1 FROM applicant_registry.compliance_flags
                    WHERE company_id = $1
                      AND is_active = TRUE
                      AND severity = 'HIGH'
                ) AS has_flag
                """,
                company_id,
            )
        return row["has_flag"] if row else False
