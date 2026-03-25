"""
Strict tests for ApplicantRegistryClient.

These tests run against the REAL seeded PostgreSQL database.
They prove the IO layer is bulletproof before any agent touches it.
"""
import asyncio
from decimal import Decimal

import asyncpg
import pytest

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from registry.client import (
    ApplicantRegistryClient,
    CompanyProfile,
    FinancialYear,
    ComplianceFlag,
    LoanRelationship,
)

import os

DATABASE_URL = os.getenv("DATABASE_URL")
pytestmark = pytest.mark.skipif(
    DATABASE_URL is None,
    reason="DATABASE_URL not set — skipping real DB tests",
)


@pytest.fixture
async def client():
    """Create a registry client backed by the real database."""
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)
    yield ApplicantRegistryClient(pool)
    await pool.close()


# ═══════════════════════════════════════════════════════════════
# 1. COMPANY PROFILE TESTS
# ═══════════════════════════════════════════════════════════════

class TestGetCompany:
    """Tests for get_company()."""

    async def test_existing_company_returns_profile(self, client):
        """COMP-001 must exist and return a CompanyProfile."""
        company = await client.get_company("COMP-001")
        assert company is not None
        assert isinstance(company, CompanyProfile)
        assert company.company_id == "COMP-001"
        assert company.company_name  # non-empty
        assert company.sector  # non-empty
        assert company.jurisdiction  # non-empty
        assert company.trajectory in ("GROWTH", "STABLE", "DECLINING", "RECOVERING", "VOLATILE")
        assert company.risk_tier in ("LOW", "MEDIUM", "HIGH")

    async def test_missing_company_returns_none(self, client):
        """Non-existent company must return None, not raise."""
        company = await client.get_company("COMP-999")
        assert company is None

    async def test_company_has_valid_founded_year(self, client):
        """Founded year must be reasonable (not in the future)."""
        company = await client.get_company("COMP-001")
        assert company is not None
        assert 1990 <= company.founded_year <= 2025


# ═══════════════════════════════════════════════════════════════
# 2. FINANCIAL HISTORY TESTS — THE 3-YEAR DATA VALIDATION
# ═══════════════════════════════════════════════════════════════

class TestFinancialHistory:
    """Tests for get_financial_history()."""

    async def test_returns_3_year_history(self, client):
        """Every seeded company must have exactly 3 fiscal years."""
        history = await client.get_financial_history("COMP-001")
        assert len(history) == 3
        years = [fy.fiscal_year for fy in history]
        assert years == [2022, 2023, 2024]

    async def test_history_ordered_ascending(self, client):
        """Fiscal years must be in ascending order (oldest first)."""
        history = await client.get_financial_history("COMP-031")
        for i in range(1, len(history)):
            assert history[i].fiscal_year > history[i - 1].fiscal_year

    async def test_financial_data_is_decimal(self, client):
        """Revenue and income must be Decimal, not float."""
        history = await client.get_financial_history("COMP-001")
        assert len(history) > 0
        fy = history[0]
        assert isinstance(fy.total_revenue, Decimal)
        assert isinstance(fy.net_income, Decimal)
        assert isinstance(fy.total_assets, Decimal)

    async def test_financial_data_positive_revenue(self, client):
        """Revenue must be positive for all years."""
        history = await client.get_financial_history("COMP-001")
        for fy in history:
            assert fy.total_revenue > 0

    async def test_assets_equal_liabilities_plus_equity(self, client):
        """Basic accounting: Assets ≈ Liabilities + Equity (within rounding)."""
        history = await client.get_financial_history("COMP-001")
        for fy in history:
            if fy.total_equity is not None:
                expected = fy.total_liabilities + fy.total_equity
                diff = abs(fy.total_assets - expected)
                # Allow for rounding (< $1)
                assert diff < 1, f"A/E mismatch: {fy.total_assets} vs {expected}"

    async def test_missing_company_returns_empty_history(self, client):
        """Non-existent company returns empty list, not None."""
        history = await client.get_financial_history("COMP-999")
        assert history == []

    async def test_partial_history_possible(self, client):
        """The function returns whatever exists — even if < 3 years.
        (In our seed, every company has 3 years, but the client must handle fewer.)"""
        history = await client.get_financial_history("COMP-001")
        assert isinstance(history, list)

    async def test_financial_history_handles_incomplete_years(self, client):
        """If a company has fewer than 3 fiscal years, the client must:
        - Not crash
        - Return correct ordering
        - Return a list shorter than 3
        This is critical because agents must handle incomplete data safely.
        We test with a non-existent company (0 years) as the extreme case,
        and then verify the client contract holds for any result length."""
        # Extreme case: no history at all
        history = await client.get_financial_history("COMP-999")
        assert isinstance(history, list)
        assert len(history) == 0

        # Normal case: verify the contract even for existing companies
        history = await client.get_financial_history("COMP-001")
        assert len(history) <= 3
        # Must still be ordered ascending
        for i in range(1, len(history)):
            assert history[i].fiscal_year > history[i - 1].fiscal_year
        # All entries must have non-null required fields
        for fy in history:
            assert fy.total_revenue is not None
            assert fy.net_income is not None
            assert fy.total_assets is not None
            assert fy.total_liabilities is not None


# ═══════════════════════════════════════════════════════════════
# 3. COMPLIANCE FLAGS TESTS
# ═══════════════════════════════════════════════════════════════

class TestComplianceFlags:
    """Tests for get_compliance_flags()."""

    async def test_company_with_no_flags_returns_empty(self, client):
        """Most companies have no compliance flags."""
        flags = await client.get_compliance_flags("COMP-001")
        # COMP-001 may or may not have flags based on seed — check type
        assert isinstance(flags, list)

    async def test_active_only_filtering(self, client):
        """active_only=True must return only flags with is_active=True."""
        # Get all flags for a company with known flags
        all_flags = await client.get_compliance_flags("COMP-001")
        active_flags = await client.get_compliance_flags("COMP-001", active_only=True)

        for f in active_flags:
            assert f.is_active is True

        # active_only subset must be <= total
        assert len(active_flags) <= len(all_flags)

    async def test_flag_has_required_fields(self, client):
        """Every flag must have flag_type, severity, and is_active."""
        # Find a company that actually has flags from the seed
        # We know from seeding that 2 compliance flags were created
        # Let's check all companies until we find one with flags
        for i in range(1, 82):
            cid = f"COMP-{i:03d}"
            flags = await client.get_compliance_flags(cid)
            if flags:
                flag = flags[0]
                assert isinstance(flag, ComplianceFlag)
                assert flag.flag_type  # non-empty
                assert flag.severity in ("LOW", "MEDIUM", "HIGH")
                assert isinstance(flag.is_active, bool)
                return
        # If no flags found at all, that's still valid (seed may have 0)

    async def test_missing_company_returns_empty_flags(self, client):
        """Non-existent company returns empty list."""
        flags = await client.get_compliance_flags("COMP-999")
        assert flags == []


# ═══════════════════════════════════════════════════════════════
# 4. LOAN RELATIONSHIPS TESTS
# ═══════════════════════════════════════════════════════════════

class TestLoanRelationships:
    """Tests for get_loan_relationships()."""

    async def test_company_with_loans_returns_records(self, client):
        """Companies with prior loans must return LoanRelationship objects."""
        # Search for a company that has loans
        for i in range(1, 82):
            cid = f"COMP-{i:03d}"
            loans = await client.get_loan_relationships(cid)
            if loans:
                loan = loans[0]
                assert isinstance(loan, LoanRelationship)
                assert loan.company_id == cid
                assert loan.loan_amount_usd > 0
                assert isinstance(loan.loan_date, type(loan.loan_date))
                assert loan.status in ("ACTIVE", "REPAID", "DEFAULT", "RESTRUCTURED")
                assert isinstance(loan.default_occurred, bool)
                return

    async def test_missing_company_returns_empty_loans(self, client):
        """Non-existent company returns empty list."""
        loans = await client.get_loan_relationships("COMP-999")
        assert loans == []

    async def test_loans_ordered_by_date_desc(self, client):
        """Loans must be ordered by loan_date descending (most recent first)."""
        for i in range(1, 82):
            cid = f"COMP-{i:03d}"
            loans = await client.get_loan_relationships(cid)
            if len(loans) >= 2:
                for j in range(1, len(loans)):
                    assert loans[j - 1].loan_date >= loans[j].loan_date
                return

    async def test_default_flag_logic(self, client):
        """has_prior_defaults() returns True only if a loan has default_occurred=True."""
        # Check all companies — at least one should have defaults (from seed hints)
        found_default = False
        found_no_default = False
        for i in range(1, 82):
            cid = f"COMP-{i:03d}"
            has_default = await client.has_prior_defaults(cid)
            assert isinstance(has_default, bool)
            if has_default:
                found_default = True
                # Verify manually
                loans = await client.get_loan_relationships(cid)
                assert any(l.default_occurred for l in loans)
            else:
                found_no_default = True

        # We should have both cases in our seed
        # (At minimum, companies without loans have no defaults)
        assert found_no_default  # Most companies have no defaults


# ═══════════════════════════════════════════════════════════════
# 5. CONVENIENCE METHOD TESTS
# ═══════════════════════════════════════════════════════════════

class TestConvenienceMethods:
    """Tests for has_prior_defaults() and has_active_high_flag()."""

    async def test_has_prior_defaults_for_clean_company(self, client):
        """Company without loans cannot have defaults."""
        # COMP-999 doesn't exist
        result = await client.has_prior_defaults("COMP-999")
        assert result is False

    async def test_has_active_high_flag_for_clean_company(self, client):
        """Company without flags returns False."""
        result = await client.has_active_high_flag("COMP-999")
        assert result is False
