#!/usr/bin/env python3
"""
Data Validation Framework
=========================
Production-grade data validation with configurable rules,
quality scoring, and detailed reporting.

Features:
- Schema validation (types, nullability)
- Completeness checks
- Uniqueness validation
- Range/bounds checking
- Pattern matching (regex)
- Referential integrity
- Quality scoring

Author: Mboya Jeffers
"""

import re
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Union
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ValidationSeverity(Enum):
    """Severity levels for validation issues."""
    ERROR = "error"      # Blocks pipeline
    WARNING = "warning"  # Logged but continues
    INFO = "info"        # Informational only


@dataclass
class ValidationRule:
    """A single validation rule."""
    name: str
    column: Optional[str]
    check: Callable[[pd.DataFrame], bool]
    severity: ValidationSeverity = ValidationSeverity.ERROR
    description: str = ""


@dataclass
class ValidationIssue:
    """A validation failure."""
    rule_name: str
    column: Optional[str]
    severity: ValidationSeverity
    message: str
    affected_rows: int = 0
    sample_values: List[Any] = field(default_factory=list)


@dataclass
class ValidationReport:
    """Complete validation report."""
    timestamp: str
    total_rows: int
    total_columns: int
    rules_checked: int
    rules_passed: int
    rules_failed: int
    quality_score: float
    issues: List[ValidationIssue]
    passed: bool

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            'timestamp': self.timestamp,
            'total_rows': self.total_rows,
            'total_columns': self.total_columns,
            'rules_checked': self.rules_checked,
            'rules_passed': self.rules_passed,
            'rules_failed': self.rules_failed,
            'quality_score': self.quality_score,
            'passed': self.passed,
            'issues': [
                {
                    'rule': i.rule_name,
                    'column': i.column,
                    'severity': i.severity.value,
                    'message': i.message,
                    'affected_rows': i.affected_rows,
                    'sample_values': i.sample_values[:5]
                }
                for i in self.issues
            ]
        }


class SchemaValidator:
    """Validate DataFrame schema against expected structure."""

    TYPE_MAP = {
        'int': ['int64', 'int32', 'Int64', 'Int32'],
        'float': ['float64', 'float32'],
        'str': ['object', 'string'],
        'bool': ['bool', 'boolean'],
        'datetime': ['datetime64[ns]', 'datetime64']
    }

    def __init__(self, schema: Dict[str, Dict[str, Any]]):
        """
        Initialize with schema definition.

        Schema format:
        {
            'column_name': {
                'type': 'int|float|str|bool|datetime',
                'nullable': True|False,
                'required': True|False
            }
        }
        """
        self.schema = schema

    def validate_columns_exist(self, df: pd.DataFrame) -> List[ValidationIssue]:
        """Check all required columns exist."""
        issues = []

        required_cols = {
            col for col, spec in self.schema.items()
            if spec.get('required', True)
        }

        missing = required_cols - set(df.columns)
        if missing:
            issues.append(ValidationIssue(
                rule_name='required_columns',
                column=None,
                severity=ValidationSeverity.ERROR,
                message=f"Missing required columns: {missing}",
                affected_rows=len(df)
            ))

        return issues

    def validate_types(self, df: pd.DataFrame) -> List[ValidationIssue]:
        """Check column data types."""
        issues = []

        for col, spec in self.schema.items():
            if col not in df.columns:
                continue

            expected_type = spec.get('type')
            if not expected_type:
                continue

            actual_type = str(df[col].dtype)
            valid_types = self.TYPE_MAP.get(expected_type, [expected_type])

            if actual_type not in valid_types:
                issues.append(ValidationIssue(
                    rule_name='column_type',
                    column=col,
                    severity=ValidationSeverity.WARNING,
                    message=f"Expected {expected_type}, got {actual_type}"
                ))

        return issues

    def validate_nullability(self, df: pd.DataFrame) -> List[ValidationIssue]:
        """Check nullable constraints."""
        issues = []

        for col, spec in self.schema.items():
            if col not in df.columns:
                continue

            nullable = spec.get('nullable', True)
            if not nullable:
                null_count = df[col].isna().sum()
                if null_count > 0:
                    issues.append(ValidationIssue(
                        rule_name='not_nullable',
                        column=col,
                        severity=ValidationSeverity.ERROR,
                        message=f"Column has {null_count} null values but is not nullable",
                        affected_rows=null_count
                    ))

        return issues

    def validate(self, df: pd.DataFrame) -> List[ValidationIssue]:
        """Run all schema validations."""
        issues = []
        issues.extend(self.validate_columns_exist(df))
        issues.extend(self.validate_types(df))
        issues.extend(self.validate_nullability(df))
        return issues


class DataQualityValidator:
    """Validate data quality metrics."""

    def __init__(self, thresholds: Optional[Dict[str, float]] = None):
        self.thresholds = thresholds or {
            'completeness': 0.95,
            'uniqueness': 0.99
        }

    def check_completeness(self, df: pd.DataFrame,
                           columns: Optional[List[str]] = None) -> List[ValidationIssue]:
        """Check data completeness (non-null ratio)."""
        issues = []
        cols = columns or df.columns.tolist()

        for col in cols:
            if col not in df.columns:
                continue

            completeness = df[col].notna().mean()
            threshold = self.thresholds.get('completeness', 0.95)

            if completeness < threshold:
                null_count = df[col].isna().sum()
                issues.append(ValidationIssue(
                    rule_name='completeness',
                    column=col,
                    severity=ValidationSeverity.WARNING,
                    message=f"Completeness {completeness:.1%} below threshold {threshold:.1%}",
                    affected_rows=null_count
                ))

        return issues

    def check_uniqueness(self, df: pd.DataFrame,
                         key_columns: List[str]) -> List[ValidationIssue]:
        """Check uniqueness of key columns."""
        issues = []

        existing_cols = [c for c in key_columns if c in df.columns]
        if not existing_cols:
            return issues

        total = len(df)
        unique = len(df.drop_duplicates(subset=existing_cols))
        uniqueness = unique / total if total > 0 else 1.0

        threshold = self.thresholds.get('uniqueness', 0.99)
        if uniqueness < threshold:
            dup_count = total - unique
            issues.append(ValidationIssue(
                rule_name='uniqueness',
                column=','.join(existing_cols),
                severity=ValidationSeverity.ERROR,
                message=f"Uniqueness {uniqueness:.1%} below threshold {threshold:.1%}",
                affected_rows=dup_count
            ))

        return issues


class ValueValidator:
    """Validate individual values against rules."""

    @staticmethod
    def check_range(df: pd.DataFrame, column: str,
                    min_val: Optional[float] = None,
                    max_val: Optional[float] = None) -> List[ValidationIssue]:
        """Check values are within expected range."""
        issues = []

        if column not in df.columns:
            return issues

        series = pd.to_numeric(df[column], errors='coerce')

        if min_val is not None:
            below_min = (series < min_val).sum()
            if below_min > 0:
                samples = df.loc[series < min_val, column].head(3).tolist()
                issues.append(ValidationIssue(
                    rule_name='range_min',
                    column=column,
                    severity=ValidationSeverity.WARNING,
                    message=f"{below_min} values below minimum {min_val}",
                    affected_rows=below_min,
                    sample_values=samples
                ))

        if max_val is not None:
            above_max = (series > max_val).sum()
            if above_max > 0:
                samples = df.loc[series > max_val, column].head(3).tolist()
                issues.append(ValidationIssue(
                    rule_name='range_max',
                    column=column,
                    severity=ValidationSeverity.WARNING,
                    message=f"{above_max} values above maximum {max_val}",
                    affected_rows=above_max,
                    sample_values=samples
                ))

        return issues

    @staticmethod
    def check_pattern(df: pd.DataFrame, column: str,
                      pattern: str) -> List[ValidationIssue]:
        """Check values match a regex pattern."""
        issues = []

        if column not in df.columns:
            return issues

        regex = re.compile(pattern)
        series = df[column].astype(str)

        non_matching = ~series.str.match(regex, na=False)
        non_match_count = non_matching.sum()

        if non_match_count > 0:
            samples = df.loc[non_matching, column].head(3).tolist()
            issues.append(ValidationIssue(
                rule_name='pattern_match',
                column=column,
                severity=ValidationSeverity.WARNING,
                message=f"{non_match_count} values don't match pattern '{pattern}'",
                affected_rows=non_match_count,
                sample_values=samples
            ))

        return issues

    @staticmethod
    def check_allowed_values(df: pd.DataFrame, column: str,
                             allowed: Set[Any]) -> List[ValidationIssue]:
        """Check values are in allowed set."""
        issues = []

        if column not in df.columns:
            return issues

        invalid = ~df[column].isin(allowed) & df[column].notna()
        invalid_count = invalid.sum()

        if invalid_count > 0:
            samples = df.loc[invalid, column].unique()[:5].tolist()
            issues.append(ValidationIssue(
                rule_name='allowed_values',
                column=column,
                severity=ValidationSeverity.WARNING,
                message=f"{invalid_count} values not in allowed set",
                affected_rows=invalid_count,
                sample_values=samples
            ))

        return issues


class DataValidator:
    """
    Main validation orchestrator.

    Usage:
        validator = DataValidator(schema={'id': {'type': 'int', 'required': True}})
        validator.add_range_check('amount', min_val=0)
        validator.add_pattern_check('email', r'^[\w\.-]+@[\w\.-]+\.\w+$')

        report = validator.validate(df)
        print(f"Quality Score: {report.quality_score:.1%}")
    """

    def __init__(self, schema: Optional[Dict] = None,
                 thresholds: Optional[Dict[str, float]] = None):
        self.schema_validator = SchemaValidator(schema or {})
        self.quality_validator = DataQualityValidator(thresholds)
        self.value_rules: List[Callable[[pd.DataFrame], List[ValidationIssue]]] = []
        self.key_columns: List[str] = []

    def set_key_columns(self, columns: List[str]) -> 'DataValidator':
        """Set columns for uniqueness check."""
        self.key_columns = columns
        return self

    def add_range_check(self, column: str,
                        min_val: Optional[float] = None,
                        max_val: Optional[float] = None) -> 'DataValidator':
        """Add a range validation rule."""
        self.value_rules.append(
            lambda df, c=column, mi=min_val, ma=max_val:
            ValueValidator.check_range(df, c, mi, ma)
        )
        return self

    def add_pattern_check(self, column: str, pattern: str) -> 'DataValidator':
        """Add a regex pattern validation rule."""
        self.value_rules.append(
            lambda df, c=column, p=pattern:
            ValueValidator.check_pattern(df, c, p)
        )
        return self

    def add_allowed_values(self, column: str,
                           allowed: Set[Any]) -> 'DataValidator':
        """Add an allowed values validation rule."""
        self.value_rules.append(
            lambda df, c=column, a=allowed:
            ValueValidator.check_allowed_values(df, c, a)
        )
        return self

    def validate(self, df: pd.DataFrame) -> ValidationReport:
        """Run all validations and generate report."""
        logger.info(f"Validating DataFrame: {len(df)} rows, {len(df.columns)} columns")

        all_issues: List[ValidationIssue] = []

        # Schema validation
        all_issues.extend(self.schema_validator.validate(df))

        # Quality validation
        all_issues.extend(self.quality_validator.check_completeness(df))
        if self.key_columns:
            all_issues.extend(
                self.quality_validator.check_uniqueness(df, self.key_columns)
            )

        # Value rules
        for rule_func in self.value_rules:
            all_issues.extend(rule_func(df))

        # Calculate quality score
        rules_checked = 3 + len(self.value_rules)  # schema + completeness + uniqueness + custom
        error_count = sum(1 for i in all_issues if i.severity == ValidationSeverity.ERROR)
        warning_count = sum(1 for i in all_issues if i.severity == ValidationSeverity.WARNING)

        # Score: errors count double
        deductions = (error_count * 2 + warning_count) / rules_checked
        quality_score = max(0, 1 - deductions * 0.1)

        # Pipeline passes if no ERROR severity issues
        passed = error_count == 0

        report = ValidationReport(
            timestamp=datetime.now(timezone.utc).isoformat(),
            total_rows=len(df),
            total_columns=len(df.columns),
            rules_checked=rules_checked,
            rules_passed=rules_checked - len(all_issues),
            rules_failed=len(all_issues),
            quality_score=round(quality_score, 3),
            issues=all_issues,
            passed=passed
        )

        logger.info(f"Validation complete: {report.rules_passed}/{report.rules_checked} passed")
        logger.info(f"Quality Score: {report.quality_score:.1%}")
        logger.info(f"Result: {'PASSED' if passed else 'FAILED'}")

        return report


# Demo
if __name__ == '__main__':
    print("=" * 50)
    print("Data Validation Framework Demo")
    print("=" * 50)

    # Create sample data with some issues
    df = pd.DataFrame({
        'id': [1, 2, 3, 4, 5, 5],  # Duplicate ID
        'name': ['Alice', 'Bob', None, 'David', 'Eve', 'Frank'],  # Null
        'email': ['alice@test.com', 'invalid-email', 'charlie@test.com',
                  'david@test.com', 'eve@test.com', 'frank@test.com'],
        'age': [25, 30, 35, -5, 45, 150],  # Out of range values
        'status': ['active', 'active', 'invalid', 'inactive', 'active', 'active']
    })

    print("\nSample Data:")
    print(df)

    # Configure validator
    schema = {
        'id': {'type': 'int', 'required': True, 'nullable': False},
        'name': {'type': 'str', 'required': True},
        'email': {'type': 'str', 'required': True},
        'age': {'type': 'int', 'required': True},
        'status': {'type': 'str', 'required': True}
    }

    validator = DataValidator(schema=schema)
    validator.set_key_columns(['id'])
    validator.add_range_check('age', min_val=0, max_val=120)
    validator.add_pattern_check('email', r'^[\w\.-]+@[\w\.-]+\.\w+$')
    validator.add_allowed_values('status', {'active', 'inactive', 'pending'})

    # Run validation
    print("\n" + "=" * 50)
    report = validator.validate(df)

    print("\n" + "=" * 50)
    print("Validation Issues:")
    for issue in report.issues:
        print(f"  [{issue.severity.value.upper()}] {issue.rule_name}")
        print(f"    Column: {issue.column}")
        print(f"    Message: {issue.message}")
        if issue.sample_values:
            print(f"    Samples: {issue.sample_values}")
