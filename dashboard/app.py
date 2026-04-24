"""
JobSpy Dashboard — Backend API Server
Run with: python dashboard/app.py
Opens at: http://localhost:5000
"""

import csv
import io
import os
import re
import sys
import threading
import time
from datetime import datetime

from flask import Flask, jsonify, request, send_file, render_template_string
from flask_cors import CORS

# Add parent dir to path so we can import jobspy
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jobspy import scrape_jobs, scrape_smart_fresher_jobs, format_hunt_results

app = Flask(__name__, static_folder="static", template_folder="templates")
CORS(app)

# In-memory store for scrape results
scrape_store = {
    "status": "idle",  # idle | running | done | error
    "progress": "",
    "jobs_df": None,
    "filtered_df": None,
    "error": None,
    "started_at": None,
    "finished_at": None,
    "config": None,
}

lock = threading.Lock()


def _normalize_jobs_dataframe(df):
    """Unify smart/report columns with the default scrape schema for API rendering."""
    if df is None:
        return df

    normalized = df.copy()
    alias_map = {
        "job_title": "title",
        "company_name": "company",
        "application_link": "job_url",
        "platforms_found": "site",
        "experience_required": "experience_range",
        "key_skills_mentioned": "skills",
    }
    for source_col, target_col in alias_map.items():
        if target_col not in normalized.columns and source_col in normalized.columns:
            normalized[target_col] = normalized[source_col]

    required_cols = [
        "site",
        "title",
        "company",
        "location",
        "date_posted",
        "job_type",
        "job_url",
        "min_amount",
        "max_amount",
        "currency",
        "stipend",
        "description",
        "experience_range",
        "skills",
    ]
    for column in required_cols:
        if column not in normalized.columns:
            normalized[column] = None

    return normalized


def _apply_fresher_filter(df):
    if df is None or df.empty:
        return df

    text_columns = [col for col in ("title", "description", "experience_range", "skills") if col in df.columns]
    if not text_columns:
        return df

    fresher_pattern = r"intern|junior|fresher|entry|associate|trainee|graduate|new grad|0-1|0-2"
    searchable = df[text_columns].fillna("").astype(str).agg(" ".join, axis=1)
    return df[searchable.str.contains(fresher_pattern, case=False, na=False)]


def _unique_sites(df):
    if df is None or df.empty or "site" not in df.columns:
        return []

    sites = set()
    for raw in df["site"].dropna().astype(str):
        for part in raw.split(","):
            site = part.strip()
            if site:
                sites.add(site)
    return sorted(sites)


def _filter_by_site(df, filter_site):
    if df is None or df.empty or not filter_site or filter_site == "all" or "site" not in df.columns:
        return df
    token = re.escape(filter_site.strip())
    pattern = rf"(?:^|,\s*){token}(?:\s*,|$)"
    return df[df["site"].fillna("").astype(str).str.contains(pattern, case=False, regex=True)]


def run_scrape(config: dict):
    """Background scrape worker"""
    global scrape_store
    with lock:
        scrape_store["status"] = "running"
        scrape_store["progress"] = "Starting scrape..."
        scrape_store["started_at"] = datetime.now().isoformat()
        scrape_store["error"] = None
        scrape_store["config"] = config

    try:
        strategy = (config.get("strategy") or "default").strip().lower()
        sites = config.get("sites", ["indeed", "linkedin"])
        search_term = config.get("search_term", "software engineer")
        location = config.get("location", "India")
        results_wanted = int(config.get("results_wanted", 50))
        hours_old = int(config.get("hours_old", 72)) if config.get("hours_old") else None
        country_indeed = config.get("country_indeed", "India")
        job_type = config.get("job_type") or None
        is_remote = config.get("is_remote", False)
        description_format = config.get("description_format", "markdown")

        with lock:
            if strategy in ("smart", "smart_fresher", "fresher_smart"):
                scrape_store["progress"] = "Running smart fresher hunt..."
            else:
                scrape_store["progress"] = f"Scraping {len(sites)} sites for '{search_term}'..."

        if strategy in ("smart", "smart_fresher", "fresher_smart"):
            top_n_combinations = int(config.get("top_n_combinations", 10))
            preferred_days_old = int(config.get("preferred_days_old", 7))
            fallback_days_old = int(config.get("fallback_days_old", 30))
            enforce_degree_filter_raw = config.get("enforce_degree_filter", True)
            if isinstance(enforce_degree_filter_raw, str):
                enforce_degree_filter = enforce_degree_filter_raw.strip().lower() in (
                    "1",
                    "true",
                    "yes",
                    "y",
                )
            else:
                enforce_degree_filter = bool(enforce_degree_filter_raw)
            output_format = (config.get("output_format") or "detailed").strip().lower()

            jobs = scrape_smart_fresher_jobs(
                top_n_combinations=top_n_combinations,
                location=location,
                site_rotation=sites,
                country_indeed=country_indeed,
                results_wanted_per_combo=results_wanted,
                preferred_days_old=preferred_days_old,
                fallback_days_old=fallback_days_old,
                enforce_degree_filter=enforce_degree_filter,
                verbose=0,
            )

            if output_format in ("report", "structured", "summary"):
                jobs = format_hunt_results(jobs)
        else:
            jobs = scrape_jobs(
                site_name=sites,
                search_term=search_term,
                location=location,
                results_wanted=results_wanted,
                hours_old=hours_old,
                country_indeed=country_indeed,
                job_type=job_type if job_type and job_type != "any" else None,
                is_remote=is_remote,
                description_format=description_format,
            )

        with lock:
            scrape_store["jobs_df"] = jobs
            scrape_store["filtered_df"] = None
            scrape_store["status"] = "done"
            scrape_store["finished_at"] = datetime.now().isoformat()
            scrape_store["progress"] = f"Found {len(jobs)} jobs"

    except Exception as e:
        with lock:
            scrape_store["status"] = "error"
            scrape_store["error"] = str(e)
            scrape_store["progress"] = f"Error: {str(e)}"
            scrape_store["finished_at"] = datetime.now().isoformat()


@app.route("/")
def index():
    return send_file("templates/index.html")


@app.route("/api/scrape", methods=["POST"])
def start_scrape():
    """Start a new scraping job"""
    if scrape_store["status"] == "running":
        return jsonify({"error": "A scrape is already running"}), 409

    config = request.json
    thread = threading.Thread(target=run_scrape, args=(config,), daemon=True)
    thread.start()
    return jsonify({"status": "started", "config": config})


@app.route("/api/status")
def get_status():
    """Get current scrape status"""
    with lock:
        result = {
            "status": scrape_store["status"],
            "progress": scrape_store["progress"],
            "error": scrape_store["error"],
            "started_at": scrape_store["started_at"],
            "finished_at": scrape_store["finished_at"],
            "job_count": len(scrape_store["jobs_df"]) if scrape_store["jobs_df"] is not None else 0,
        }
    return jsonify(result)


@app.route("/api/jobs")
def get_jobs():
    """Get scraped jobs as JSON"""
    with lock:
        df = scrape_store["jobs_df"]
    if df is None or df.empty:
        return jsonify({"jobs": [], "total": 0})

    # Apply filters from query params
    filtered = _normalize_jobs_dataframe(df)

    filter_text = request.args.get("filter", "").strip()
    if filter_text:
        mask = filtered.apply(
            lambda row: row.astype(str).str.contains(filter_text, case=False, na=False).any(),
            axis=1,
        )
        filtered = filtered[mask]

    filter_site = request.args.get("site", "").strip()
    filtered = _filter_by_site(filtered, filter_site)

    fresher_only = request.args.get("fresher", "").strip().lower() == "true"
    if fresher_only:
        filtered = _apply_fresher_filter(filtered)

    # Pagination
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 25))
    total = len(filtered)
    start = (page - 1) * per_page
    end = start + per_page
    paginated = filtered.iloc[start:end]

    # Convert to records
    records = []
    for _, row in paginated.iterrows():
        record = {}
        for col in paginated.columns:
            val = row[col]
            if val is None or (isinstance(val, float) and str(val) == "nan"):
                record[col] = None
            else:
                record[col] = str(val) if not isinstance(val, (str, int, float, bool)) else val
        records.append(record)

    sites_list = _unique_sites(_normalize_jobs_dataframe(df))

    return jsonify({
        "jobs": records,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page,
        "sites": sites_list,
    })


@app.route("/api/download")
def download_csv():
    """Download results as CSV"""
    with lock:
        df = scrape_store["jobs_df"]
    if df is None or df.empty:
        return jsonify({"error": "No data to download"}), 404

    df = _normalize_jobs_dataframe(df)
    fresher_only = request.args.get("fresher", "").strip().lower() == "true"
    if fresher_only:
        df = _apply_fresher_filter(df)

    output = io.StringIO()
    df.to_csv(output, quoting=csv.QUOTE_NONNUMERIC, escapechar="\\", index=False)
    output.seek(0)

    filename = "fresher_jobs.csv" if fresher_only else "all_jobs.csv"

    mem = io.BytesIO()
    mem.write(output.getvalue().encode("utf-8"))
    mem.seek(0)

    return send_file(
        mem,
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename,
    )


@app.route("/api/save", methods=["POST"])
def save_csv():
    """Save results to disk"""
    with lock:
        df = scrape_store["jobs_df"]
    if df is None or df.empty:
        return jsonify({"error": "No data to save"}), 404

    df = _normalize_jobs_dataframe(df)
    save_dir = request.json.get("directory", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    # Save all jobs
    all_path = os.path.join(save_dir, "all_jobs.csv")
    df.to_csv(all_path, quoting=csv.QUOTE_NONNUMERIC, escapechar="\\", index=False)

    # Save fresher-filtered jobs
    fresher_df = _apply_fresher_filter(df)
    fresher_path = os.path.join(save_dir, "fresher_jobs.csv")
    fresher_df.to_csv(fresher_path, quoting=csv.QUOTE_NONNUMERIC, escapechar="\\", index=False)

    return jsonify({
        "saved": True,
        "files": [
            {"path": all_path, "count": len(df)},
            {"path": fresher_path, "count": len(fresher_df)},
        ],
    })


@app.route("/api/reset", methods=["POST"])
def reset():
    """Reset the scrape store"""
    global scrape_store
    with lock:
        scrape_store = {
            "status": "idle",
            "progress": "",
            "jobs_df": None,
            "filtered_df": None,
            "error": None,
            "started_at": None,
            "finished_at": None,
            "config": None,
        }
    return jsonify({"status": "reset"})


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("JobSpy Dashboard")
    print("Open in browser: http://localhost:5000")
    print("=" * 60 + "\n")
    app.run(debug=False, port=5000, host="0.0.0.0")
