"""
Fresher job filtering module for CareerRadar.

This module provides filtering logic for fresher-level SDE roles,
separating scraping concerns from filtering concerns.
"""

from __future__ import annotations

import re
from typing import Tuple

import pandas as pd


# =============================================================================
# NEGATIVE PATTERNS - Jobs matching these are dropped
# =============================================================================
NEGATIVE_PATTERNS = [
    re.compile(r"\b[2-9]\+?\s*years?\s*(of\s*)?(experience|exp)\b", re.IGNORECASE),
    re.compile(r"\bminimum\s+[1-9]\s+years?\b", re.IGNORECASE),
    re.compile(r"\bsenior\b", re.IGNORECASE),
    re.compile(r"\bsr\.\b", re.IGNORECASE),
    re.compile(r"\blead\b", re.IGNORECASE),
    re.compile(r"\bstaff\s+engineer\b", re.IGNORECASE),
    re.compile(r"\bprincipal\b", re.IGNORECASE),
    re.compile(r"\bmanager\b", re.IGNORECASE),
    re.compile(r"\bdirector\b", re.IGNORECASE),
    re.compile(r"\bprevious\s+experience\s+required\b", re.IGNORECASE),
    re.compile(r"\bproven\s+track\s+record\b", re.IGNORECASE),
]


# =============================================================================
# POSITIVE PATTERNS - Jobs matching these get signal tags and scoring
# =============================================================================
POSITIVE_PATTERNS = [
    re.compile(r"\b0[\s\-]?1\s+years?\b", re.IGNORECASE),
    re.compile(r"\bfreshers?\s+welcome\b", re.IGNORECASE),
    re.compile(r"\bno\s+experience\s+required\b", re.IGNORECASE),
    re.compile(r"\bcampus\s+hir(ing|e)\b", re.IGNORECASE),
    re.compile(r"\bnew\s+grad(uate)?\b", re.IGNORECASE),
    re.compile(r"\bentry[\s\-]level\b", re.IGNORECASE),
    re.compile(r"\bbatch\s+of\s+20(2[4-9]|3\d)\b", re.IGNORECASE),
    re.compile(r"\b20(2[4-9]|3\d)\s+(batch|graduate|passout)\b", re.IGNORECASE),
    re.compile(r"\brecent\s+graduate\b", re.IGNORECASE),
    re.compile(r"\bjust\s+graduated\b", re.IGNORECASE),
]


def is_negative_match(text: str) -> bool:
    """
    Check if the given text matches any negative pattern.
    
    Args:
        text: The text to check (typically job title + description)
        
    Returns:
        True if text matches any negative pattern, False otherwise
    """
    if not text:
        return False
    
    for pattern in NEGATIVE_PATTERNS:
        if pattern.search(text):
            return True
    return False


def score_positive_signals(text: str) -> Tuple[list[str], int]:
    """
    Score a job posting for fresher-friendly signals.
    
    Args:
        text: The text to analyze (typically job title + description)
        
    Returns:
        Tuple of (list of matched signal phrases, total signal count)
    """
    if not text:
        return [], 0
    
    matched_signals: list[str] = []
    
    for pattern in POSITIVE_PATTERNS:
        match = pattern.search(text)
        if match:
            matched_signals.append(match.group(0))
    
    return matched_signals, len(matched_signals)


def filter_fresher_jobs(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """
    Filter a DataFrame of jobs for fresher-level SDE roles.
    
    This function:
    1. Drops jobs that match negative patterns (senior, experience requirements, etc.)
    2. Adds fresher_signals column with matched positive patterns
    3. Adds fresher_score column with count of positive signals
    4. Optionally logs which jobs were dropped and why
    
    Args:
        df: DataFrame containing job data with 'title' and 'description' columns
        verbose: If True, logs dropped jobs and reasons
        
    Returns:
        Filtered DataFrame with fresher_signals and fresher_score columns added
    """
    if df.empty:
        print("Fetched 0 jobs -> 0 passed fresher filter")
        return df
    
    original_count = len(df)
    kept_indices = []
    fresher_signals_list = []
    fresher_scores_list = []
    dropped_log = []
    
    for idx, row in df.iterrows():
        title = str(row.get("title", ""))
        description = str(row.get("description", ""))
        combined_text = f"{title} {description}"
        
        # Check for negative patterns
        if is_negative_match(combined_text):
            if verbose:
                matched_patterns = []
                for pattern in NEGATIVE_PATTERNS:
                    match = pattern.search(combined_text)
                    if match:
                        matched_patterns.append(match.group(0))
                dropped_log.append(
                    f"DROPPED: '{title[:60]}...' - matches: {matched_patterns}"
                )
            continue
        
        # Score positive signals
        signals, score = score_positive_signals(combined_text)
        
        kept_indices.append(idx)
        fresher_signals_list.append(", ".join(signals) if signals else "")
        fresher_scores_list.append(score)
    
    # Create filtered DataFrame
    filtered_df = df.loc[kept_indices].copy()
    
    # Add new columns
    if len(filtered_df) > 0:
        filtered_df["fresher_signals"] = fresher_signals_list
        filtered_df["fresher_score"] = fresher_scores_list
    else:
        filtered_df["fresher_signals"] = pd.Series(dtype="object")
        filtered_df["fresher_score"] = pd.Series(dtype="int64")
    
    passed_count = len(filtered_df)
    print(f"Fetched {original_count} jobs -> {passed_count} passed fresher filter")
    
    if verbose and dropped_log:
        print("\n--- Dropped Jobs ---")
        for log_entry in dropped_log[:20]:  # Limit to first 20 to avoid spam
            print(log_entry)
        if len(dropped_log) > 20:
            print(f"... and {len(dropped_log) - 20} more")
    
    return filtered_df
