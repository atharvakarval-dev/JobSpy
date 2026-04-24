from __future__ import annotations

import random
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from itertools import cycle
from typing import Any

import pandas as pd

from career_radar.fresher_filter import filter_fresher_jobs
from career_radar.model import Site
from career_radar.util import map_str_to_site


# Keyword bank
JOB_TITLE_BANK = [
    "Software Development Engineer",
    "Software Engineer",
    "Junior Software Engineer",
    "Associate Software Engineer",
    "Graduate Engineer Trainee",
    "Software Developer",
    "Full Stack Developer",
    "Backend Developer",
    "Frontend Developer",
    "Programmer Analyst",
    "Technology Analyst",
    "Engineer Trainee",
    "SDE",
    "SWE",
    "Entry Level Software Engineer",
]

LANGUAGE_SKILL_BANK = [
    "Java",
    "Python",
    "C++",
    "JavaScript",
    "TypeScript",
    "Go",
    "Kotlin",
    "React",
    "Node.js",
    "Spring Boot",
    "Django",
    "Flask",
    "FastAPI",
    "Angular",
    "Vue.js",
]

CORE_CS_SKILL_BANK = [
    "Data Structures",
    "Algorithms",
    "OOP",
    "DBMS",
    "Operating Systems",
    "Computer Networks",
    "System Design",
    "DSA",
]

TOOLS_INFRA_BANK = [
    "Git",
    "Docker",
    "AWS",
    "SQL",
    "REST API",
    "MySQL",
    "MongoDB",
    "Linux",
    "CI/CD",
    "Kubernetes",
]

EXPERIENCE_QUALIFIER_BANK = [
    "Fresher",
    "0-1 years",
    "0-2 years",
    "Entry Level",
    "New Grad",
    "Recent Graduate",
    "Campus Hire",
    "2024 Batch",
    "2025 Batch",
]

DEFAULT_SITE_ROTATION = [
    Site.LINKEDIN,
    Site.NAUKRI,
    Site.INDEED,
    Site.INTERNSHALA,
    Site.GLASSDOOR,
    Site.FOUNDIT,
    Site.SHINE,
    Site.TIMESJOBS,
]

DEFAULT_DEGREE_KEYWORDS = [
    "B.Tech",
    "BE",
    "B.E",
    "MCA",
    "BCA",
    "B.Sc",
    "Computer Science",
    "CS/IT",
    "Information Technology",
]

EXPERIENCE_NEGATIVE_PATTERN = re.compile(
    r"\b("
    r"[3-9]\+?\s*years?"
    r"|[3-9]\s*(?:-|to)\s*[0-9]+\s*years?"
    r"|senior|lead|manager|principal|staff|architect"
    r")\b",
    re.IGNORECASE,
)

EXPERIENCE_POSITIVE_PATTERN = re.compile(
    r"\b("
    r"fresher|entry[\s-]?level|new\s+grad|recent\s+graduate|campus\s+hire"
    r"|0\s*(?:-|to)\s*1\s*years?"
    r"|0\s*(?:-|to)\s*2\s*years?"
    r"|1\s*(?:-|to)\s*2\s*years?"
    r"|2024\s*batch|2025\s*batch"
    r")\b",
    re.IGNORECASE,
)

TITLE_ENTRY_LEVEL_PATTERN = re.compile(
    r"\b(junior|associate|trainee|graduate|fresher|entry[\s-]?level|sde[\s-]?1)\b",
    re.IGNORECASE,
)

ALL_KEYWORD_BANK = (
    JOB_TITLE_BANK
    + LANGUAGE_SKILL_BANK
    + CORE_CS_SKILL_BANK
    + TOOLS_INFRA_BANK
    + EXPERIENCE_QUALIFIER_BANK
)


@dataclass(frozen=True)
class SearchCombination:
    combo_id: str
    query: str
    site: Site
    location: str | None = None


def _normalize_site(site: str | Site) -> Site:
    if isinstance(site, Site):
        return site
    return map_str_to_site(site.strip().lower())


def _column_or_default(df: pd.DataFrame, column: str, default: Any = None) -> pd.Series:
    if column in df.columns:
        return df[column]
    return pd.Series([default] * len(df), index=df.index)


def _ensure_columns(df: pd.DataFrame, columns: list[str], default: Any = None) -> pd.DataFrame:
    for column in columns:
        if column not in df.columns:
            df[column] = default
    return df


def _first_non_null(series: pd.Series):
    for value in series:
        if pd.notna(value):
            return value
    return None


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _is_recent_enough(posted_date: date | None, fallback_days_old: int) -> bool:
    if posted_date is None or pd.isna(posted_date):
        return True
    return posted_date >= datetime.now().date() - timedelta(days=fallback_days_old)


def _job_type_preference(job_type_value: Any) -> int:
    text = str(job_type_value or "").lower()
    if "full" in text:
        return 2
    if "intern" in text:
        return 1
    return 0


def _experience_ok(row: pd.Series) -> bool:
    text = " ".join(
        [
            str(row.get("title", "")),
            str(row.get("description", "")),
            str(row.get("experience_range", "")),
        ]
    )

    if EXPERIENCE_NEGATIVE_PATTERN.search(text):
        return False

    if EXPERIENCE_POSITIVE_PATTERN.search(text):
        return True

    if int(row.get("fresher_score", 0) or 0) > 0:
        return True

    return bool(TITLE_ENTRY_LEVEL_PATTERN.search(str(row.get("title", ""))))


def _degree_ok(
    description: Any,
    degree_keywords: list[str],
    enforce_degree_filter: bool,
) -> bool:
    if not enforce_degree_filter:
        return True

    text = str(description or "").strip()
    if not text:
        return True

    text_lower = text.lower()
    return any(keyword.lower() in text_lower for keyword in degree_keywords)


def match_keywords(text: str, keyword_bank: list[str] | None = None) -> list[str]:
    bank = keyword_bank or ALL_KEYWORD_BANK
    normalized_text = _normalize_text(text)
    matches: list[str] = []
    for keyword in bank:
        keyword_lower = keyword.lower()
        # Use stricter word boundaries for plain alpha-numeric keywords (e.g., "Go", "Java"),
        # and fallback to substring checks for punctuated multi-token phrases (e.g., "Node.js").
        if re.fullmatch(r"[a-z0-9]+", keyword_lower):
            pattern = re.compile(rf"(?<!\w){re.escape(keyword_lower)}(?!\w)")
            if pattern.search(normalized_text):
                matches.append(keyword)
        elif keyword_lower in normalized_text:
            matches.append(keyword)
    # Preserve order while deduplicating
    return list(dict.fromkeys(matches))


def _keyword_matches_for_row(row: pd.Series) -> list[str]:
    text = " ".join(
        [
            str(row.get("title", "")),
            str(row.get("description", "")),
            str(row.get("skills", "")),
            str(row.get("experience_range", "")),
            str(row.get("combo_query", "")),
        ]
    )
    return match_keywords(text)


def generate_search_combinations(
    top_n: int = 10,
    location: str | None = "India",
    site_rotation: list[str | Site] | None = None,
    seed: int = 42,
    job_titles: list[str] | None = None,
    language_skills: list[str] | None = None,
    core_cs_skills: list[str] | None = None,
    tools_infra_skills: list[str] | None = None,
    experience_qualifiers: list[str] | None = None,
) -> list[SearchCombination]:
    """
    Builds search combinations that follow a fresher-hunt strategy:
    - 1 title
    - 1-2 skills
    - 1 experience qualifier
    - alternates broad and specific searches
    """
    if top_n <= 0:
        return []

    titles = list(JOB_TITLE_BANK if job_titles is None else job_titles)
    langs = list(LANGUAGE_SKILL_BANK if language_skills is None else language_skills)
    core = list(CORE_CS_SKILL_BANK if core_cs_skills is None else core_cs_skills)
    infra = list(TOOLS_INFRA_BANK if tools_infra_skills is None else tools_infra_skills)
    experience = list(
        EXPERIENCE_QUALIFIER_BANK if experience_qualifiers is None else experience_qualifiers
    )
    sites = [_normalize_site(site) for site in (site_rotation or DEFAULT_SITE_ROTATION)]

    if not titles:
        raise ValueError("job_titles must contain at least one title")
    if not langs:
        raise ValueError("language_skills must contain at least one skill")
    if not experience:
        raise ValueError("experience_qualifiers must contain at least one value")
    if not sites:
        raise ValueError("site_rotation must contain at least one site")

    rng = random.Random(seed)
    rng.shuffle(titles)
    rng.shuffle(langs)
    rng.shuffle(core)
    rng.shuffle(infra)
    rng.shuffle(experience)

    site_cycle = cycle(sites)
    combinations: list[SearchCombination] = []

    for idx in range(top_n):
        title = titles[idx % len(titles)]
        exp = experience[idx % len(experience)]
        language_skill = langs[idx % len(langs)]
        skill_parts = [language_skill]

        # Alternate broad and specific combinations.
        if idx % 2 == 1:
            if idx % 4 == 1:
                skill_parts.append(core[idx % len(core)])
            else:
                skill_parts.append(infra[idx % len(infra)])

        query_parts = [title] + skill_parts + [exp]
        if location:
            query_parts.append(location)
        query = " ".join(query_parts)

        combinations.append(
            SearchCombination(
                combo_id=f"C{idx + 1}",
                query=query,
                site=next(site_cycle),
                location=location,
            )
        )

    return combinations


def _scrape_single_combination(
    combo: SearchCombination,
    *,
    hours_old: int,
    country_indeed: str,
    results_wanted_per_combo: int,
    verbose: int,
    scrape_kwargs: dict[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    from career_radar import scrape_jobs

    metadata = {
        "combo_id": combo.combo_id,
        "query": combo.query,
        "site": combo.site.value,
        "hours_old": hours_old,
        "status": "ok",
        "error": None,
        "count": 0,
    }

    try:
        df = scrape_jobs(
            site_name=combo.site.value,
            search_term=combo.query,
            location=combo.location,
            results_wanted=results_wanted_per_combo,
            hours_old=hours_old,
            country_indeed=country_indeed,
            country=country_indeed if country_indeed.lower() == "india" else None,
            verbose=verbose,
            **scrape_kwargs,
        )

        if df is None:
            df = pd.DataFrame()

        if not df.empty:
            df = df.copy()
            df["combo_id"] = combo.combo_id
            df["combo_query"] = combo.query
            df["combo_site"] = combo.site.value

        metadata["count"] = len(df)
        return df, metadata
    except Exception as exc:  # pragma: no cover - defensive runtime path
        metadata["status"] = "error"
        metadata["error"] = str(exc)
        return pd.DataFrame(), metadata


def _run_combinations(
    combinations: list[SearchCombination],
    *,
    hours_old: int,
    country_indeed: str,
    results_wanted_per_combo: int,
    verbose: int,
    scrape_kwargs: dict[str, Any],
) -> tuple[list[pd.DataFrame], list[dict[str, Any]]]:
    if not combinations:
        return [], []

    frames: list[pd.DataFrame] = []
    metadata: list[dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=min(len(combinations), 8)) as executor:
        future_map = {
            executor.submit(
                _scrape_single_combination,
                combo,
                hours_old=hours_old,
                country_indeed=country_indeed,
                results_wanted_per_combo=results_wanted_per_combo,
                verbose=verbose,
                scrape_kwargs=scrape_kwargs,
            ): combo
            for combo in combinations
        }

        for future in as_completed(future_map):
            combo_frame, combo_meta = future.result()
            frames.append(combo_frame)
            metadata.append(combo_meta)

    return frames, metadata


def _post_process_hunt_results(
    jobs_df: pd.DataFrame,
    *,
    preferred_days_old: int,
    fallback_days_old: int,
    enforce_degree_filter: bool,
    degree_keywords: list[str],
) -> pd.DataFrame:
    if jobs_df.empty:
        return jobs_df

    # Existing fresher-specific filtering.
    jobs_df = filter_fresher_jobs(jobs_df, verbose=False)
    if jobs_df.empty:
        return jobs_df

    processed = jobs_df.copy()
    processed = _ensure_columns(
        processed,
        [
            "title",
            "company",
            "location",
            "experience_range",
            "skills",
            "job_url",
            "job_url_direct",
            "date_posted",
            "min_amount",
            "max_amount",
            "currency",
            "job_type",
            "description",
            "site",
            "combo_id",
            "combo_query",
            "fresher_score",
            "fresher_signals",
        ],
        default=None,
    )

    processed["date_posted"] = pd.to_datetime(
        _column_or_default(processed, "date_posted"),
        errors="coerce",
    ).dt.date
    processed["posted_within_7_days"] = processed["date_posted"].apply(
        lambda d: bool(
            pd.notna(d) and d >= datetime.now().date() - timedelta(days=preferred_days_old)
        )
    )
    processed["posted_within_30_days"] = processed["date_posted"].apply(
        lambda d: bool(
            pd.notna(d) and d >= datetime.now().date() - timedelta(days=fallback_days_old)
        )
    )

    processed = processed[
        processed["date_posted"].apply(
            lambda d: _is_recent_enough(d, fallback_days_old)
        )
    ]
    if processed.empty:
        return processed

    processed = processed[processed.apply(_experience_ok, axis=1)]
    if processed.empty:
        return processed

    processed = processed[
        _column_or_default(processed, "description").apply(
            lambda d: _degree_ok(
                description=d,
                degree_keywords=degree_keywords,
                enforce_degree_filter=enforce_degree_filter,
            )
        )
    ]
    if processed.empty:
        return processed

    processed["job_type_pref"] = _column_or_default(processed, "job_type").apply(
        _job_type_preference
    )

    processed["_norm_title"] = _column_or_default(processed, "title", "").apply(
        _normalize_text
    )
    processed["_norm_company"] = _column_or_default(processed, "company", "").apply(
        _normalize_text
    )
    processed["_norm_location"] = _column_or_default(processed, "location", "").apply(
        _normalize_text
    )
    processed["_dedupe_key"] = (
        _column_or_default(processed, "job_url", "").fillna("").astype(str).str.strip()
    )
    processed.loc[processed["_dedupe_key"] == "", "_dedupe_key"] = (
        processed["_norm_title"]
        + "|"
        + processed["_norm_company"]
        + "|"
        + processed["_norm_location"]
    )

    processed["keyword_hits"] = processed.apply(_keyword_matches_for_row, axis=1)
    processed["keywords_matched"] = processed["keyword_hits"].apply(lambda values: ", ".join(values))
    processed["keyword_match_count"] = processed["keyword_hits"].apply(len)

    grouped = (
        processed.groupby("_dedupe_key", as_index=False)
        .agg(
            {
                "title": _first_non_null,
                "company": _first_non_null,
                "location": _first_non_null,
                "experience_range": _first_non_null,
                "skills": _first_non_null,
                "job_url": _first_non_null,
                "job_url_direct": _first_non_null,
                "date_posted": "max",
                "min_amount": _first_non_null,
                "max_amount": _first_non_null,
                "currency": _first_non_null,
                "job_type": _first_non_null,
                "description": _first_non_null,
                "site": lambda values: ", ".join(sorted({str(v) for v in values if pd.notna(v)})),
                "combo_id": lambda values: ", ".join(
                    sorted({str(v) for v in values if pd.notna(v)})
                ),
                "combo_query": lambda values: " || ".join(
                    sorted({str(v) for v in values if pd.notna(v)})
                ),
                "fresher_score": "max",
                "fresher_signals": _first_non_null,
                "keyword_match_count": "max",
                "keywords_matched": _first_non_null,
                "job_type_pref": "max",
                "posted_within_7_days": "max",
                "posted_within_30_days": "max",
            }
        )
        .reset_index(drop=True)
    )

    grouped["platform_count"] = grouped["site"].apply(
        lambda value: len([item for item in str(value).split(",") if item.strip()])
    )
    grouped["high_match"] = grouped["platform_count"] >= 2

    grouped["match_score"] = (
        grouped["fresher_score"].fillna(0).astype(float)
        + grouped["keyword_match_count"].fillna(0).astype(float) * 0.25
        + grouped["job_type_pref"].fillna(0).astype(float)
        + grouped["posted_within_7_days"].astype(int) * 2.0
        + grouped["posted_within_30_days"].astype(int) * 1.0
        + grouped["high_match"].astype(int) * 2.0
    )

    grouped = grouped.sort_values(
        by=["high_match", "posted_within_7_days", "match_score", "date_posted"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)

    return grouped


def _format_salary(min_amount: Any, max_amount: Any, currency: Any) -> str | None:
    if pd.isna(min_amount) and pd.isna(max_amount):
        return None
    curr = str(currency or "").strip()
    if pd.notna(min_amount) and pd.notna(max_amount):
        return f"{curr} {min_amount} - {max_amount}".strip()
    if pd.notna(min_amount):
        return f"{curr} {min_amount}".strip()
    return f"{curr} {max_amount}".strip()


def format_hunt_results(results_df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts smart-hunt results to a reporting-friendly format.
    """
    if results_df.empty:
        return results_df

    formatted = results_df.copy()
    formatted = _ensure_columns(
        formatted,
        [
            "title",
            "company",
            "location",
            "experience_range",
            "skills",
            "job_url",
            "date_posted",
            "min_amount",
            "max_amount",
            "currency",
            "keywords_matched",
            "site",
            "combo_id",
            "combo_query",
            "high_match",
            "match_score",
        ],
        default=None,
    )
    formatted["salary_ctc"] = formatted.apply(
        lambda row: _format_salary(
            row.get("min_amount"),
            row.get("max_amount"),
            row.get("currency"),
        ),
        axis=1,
    )

    return formatted[
        [
            "title",
            "company",
            "location",
            "experience_range",
            "skills",
            "job_url",
            "date_posted",
            "salary_ctc",
            "keywords_matched",
            "site",
            "combo_id",
            "combo_query",
            "high_match",
            "match_score",
        ]
    ].rename(
        columns={
            "title": "job_title",
            "company": "company_name",
            "experience_range": "experience_required",
            "skills": "key_skills_mentioned",
            "job_url": "application_link",
            "date_posted": "date_posted",
            "keywords_matched": "keywords_matched",
            "site": "platforms_found",
            "combo_id": "combination_ids",
            "combo_query": "combination_queries",
        }
    )


def scrape_smart_fresher_jobs(
    *,
    top_n_combinations: int = 10,
    location: str = "India",
    site_rotation: list[str | Site] | None = None,
    search_combinations: list[str] | None = None,
    country_indeed: str = "India",
    results_wanted_per_combo: int = 20,
    preferred_days_old: int = 7,
    fallback_days_old: int = 30,
    enforce_degree_filter: bool = True,
    degree_keywords: list[str] | None = None,
    verbose: int = 0,
    **scrape_kwargs,
) -> pd.DataFrame:
    """
    End-to-end fresher hunt strategy:
    1. Build or accept keyword combinations.
    2. Rotate combinations across sites.
    3. Scrape combinations concurrently.
    4. Retry empty combinations using a wider recency window.
    5. Apply fresher + entry-level + degree + recency filters.
    6. Deduplicate and score high-quality matches.
    """
    degree_keywords = list(degree_keywords or DEFAULT_DEGREE_KEYWORDS)
    sites = [_normalize_site(site) for site in (site_rotation or DEFAULT_SITE_ROTATION)]

    if search_combinations:
        combo_site_cycle = cycle(sites)
        combinations = [
            SearchCombination(
                combo_id=f"C{idx + 1}",
                query=query,
                site=next(combo_site_cycle),
                location=location,
            )
            for idx, query in enumerate(search_combinations)
        ]
    else:
        combinations = generate_search_combinations(
            top_n=top_n_combinations,
            location=location,
            site_rotation=sites,
        )

    preferred_hours = preferred_days_old * 24
    fallback_hours = fallback_days_old * 24

    frames, metadata = _run_combinations(
        combinations,
        hours_old=preferred_hours,
        country_indeed=country_indeed,
        results_wanted_per_combo=results_wanted_per_combo,
        verbose=verbose,
        scrape_kwargs=scrape_kwargs,
    )

    retry_combos = []
    for combo in combinations:
        combo_meta = next((m for m in metadata if m["combo_id"] == combo.combo_id), None)
        if combo_meta and combo_meta["count"] == 0 and combo_meta["status"] == "ok":
            retry_combos.append(combo)

    if retry_combos and fallback_days_old > preferred_days_old:
        retry_frames, retry_metadata = _run_combinations(
            retry_combos,
            hours_old=fallback_hours,
            country_indeed=country_indeed,
            results_wanted_per_combo=results_wanted_per_combo,
            verbose=verbose,
            scrape_kwargs=scrape_kwargs,
        )
        frames.extend(retry_frames)
        metadata.extend(retry_metadata)

    non_empty_frames = [frame for frame in frames if isinstance(frame, pd.DataFrame) and not frame.empty]
    if not non_empty_frames:
        empty = pd.DataFrame()
        empty.attrs["smart_hunt_metadata"] = metadata
        return empty

    raw_df = pd.concat(non_empty_frames, ignore_index=True)
    processed_df = _post_process_hunt_results(
        raw_df,
        preferred_days_old=preferred_days_old,
        fallback_days_old=fallback_days_old,
        enforce_degree_filter=enforce_degree_filter,
        degree_keywords=degree_keywords,
    )

    processed_df.attrs["smart_hunt_metadata"] = metadata
    processed_df.attrs["smart_hunt_combinations"] = [
        {
            "combo_id": combo.combo_id,
            "query": combo.query,
            "site": combo.site.value,
            "location": combo.location,
        }
        for combo in combinations
    ]
    return processed_df
