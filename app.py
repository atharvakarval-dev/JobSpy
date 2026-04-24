import streamlit as st
import pandas as pd
from datetime import datetime
import os
import sys
import re
import math
from pathlib import Path

# Add the directory containing app.py to sys.path
current_dir = Path(__file__).parent.absolute()
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))

try:
    from career_radar import scrape_jobs, scrape_smart_fresher_jobs, format_hunt_results
except ImportError as e:
    st.error(f"Failed to import CareerRadar modules: {e}")
    st.stop()


def build_smart_queries(search_term: str) -> list[str]:
    """Builds user-guided search combinations for Smart Fresher Hunt."""
    cleaned = (search_term or "").strip()
    if not cleaned:
        return []

    parts = [
        part.strip()
        for part in re.split(r"\s+(?:OR|or)\s+|,|\n|\|", cleaned)
        if part.strip()
    ]

    base_terms = [cleaned] + parts
    variants: list[str] = []
    for term in base_terms:
        term_lower = term.lower()
        variants.append(term)
        if "fresher" not in term_lower:
            variants.append(f"{term} fresher")
        if "entry level" not in term_lower:
            variants.append(f"{term} entry level")
        if "junior" not in term_lower:
            variants.append(f"{term} junior")
        if "new grad" not in term_lower:
            variants.append(f"{term} new grad")
        if "0-2 years" not in term_lower:
            variants.append(f"{term} 0-2 years")

    unique_queries: list[str] = []
    seen: set[str] = set()
    for query in variants:
        normalized = " ".join(query.split()).strip()
        if normalized and normalized.lower() not in seen:
            seen.add(normalized.lower())
            unique_queries.append(normalized)
    return unique_queries[:20]

# Set page config
st.set_page_config(
    page_title="CareerRadar Dashboard",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for high-end SaaS look
st.markdown("""
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&display=swap" rel="stylesheet">
<style>
    /* Global Styles */
    .stApp, .main, .stSidebar, [data-testid="stHeader"] {
        font-family: 'Outfit', sans-serif !important;
    }

    /* CRITICAL FIX: Exempt icons from font override */
    span[data-testid="stIcon"], 
    [class*="material-icons"], 
    .notranslate, 
    button[aria-label*="sidebar"] span {
        font-family: inherit !important;
    }
    
    .stApp {
        background: radial-gradient(circle at 20% 20%, #1a1a1a 0%, #0a0a0a 100%);
    }

    /* Mesh Background */
    .stApp::before {
        content: "";
        position: fixed;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background: radial-gradient(circle at 10% 10%, rgba(46, 81, 248, 0.05) 0%, transparent 40%),
                    radial-gradient(circle at 90% 90%, rgba(138, 43, 226, 0.05) 0%, transparent 40%);
        pointer-events: none;
        z-index: -1;
    }

    /* Sidebar Fixes */
    section[data-testid="stSidebar"] {
        background-color: #0e0e0e !important;
        border-right: 1px solid rgba(255, 255, 255, 0.05);
        padding-top: 1rem;
    }

    /* Prevent icon text issues */
    .st-emotion-cache-1vt458s, .st-emotion-cache-1v0vkay, [data-testid="stIcon"] {
        font-family: inherit !important;
    }

    /* Header Styling */
    h1 {
        font-weight: 800 !important;
        letter-spacing: -0.05em !important;
        background: linear-gradient(135deg, #ffffff 30%, #2E51F8 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0 !important;
    }

    .subtitle {
        color: rgba(255, 255, 255, 0.5);
        font-size: 1.1rem;
        margin-bottom: 2rem;
    }

    /* Button Styling */
    .stButton>button {
        width: 100%;
        background: linear-gradient(135deg, #2E51F8 0%, #1a3cb3 100%);
        color: white;
        border: none;
        border-radius: 12px;
        padding: 0.75rem 1rem;
        font-weight: 600;
        box-shadow: 0 4px 15px rgba(46, 81, 248, 0.2);
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    }
    
    .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 25px rgba(46, 81, 248, 0.4);
        background: linear-gradient(135deg, #3b5ef9 0%, #2E51F8 100%);
    }

    /* Card/Panel Styling */
    .glass-card {
        background: rgba(255, 255, 255, 0.03);
        border: 1px solid rgba(255, 255, 255, 0.05);
        border-radius: 16px;
        padding: 1.5rem;
        backdrop-filter: blur(10px);
        margin-bottom: 1rem;
    }

    /* Dataframe override */
    div[data-testid="stDataFrame"] {
        border: 1px solid rgba(255, 255, 255, 0.05);
        border-radius: 16px;
        overflow: hidden;
    }

    /* Metric Styling */
    [data-testid="stMetric"] {
        background: rgba(255, 255, 255, 0.03);
        border: 1px solid rgba(255, 255, 255, 0.05);
        padding: 1rem;
        border-radius: 12px;
    }

    /* Input Styling */
    .stTextInput>div>div>input, .stSelectbox>div>div>div {
        background-color: rgba(255, 255, 255, 0.03) !important;
        border: 1px solid rgba(255, 255, 255, 0.1) !important;
        border-radius: 8px !important;
    }

    /* Scrollbar */
    ::-webkit-scrollbar {
        width: 8px;
    }
    ::-webkit-scrollbar-track {
        background: rgba(0,0,0,0.1);
    }
    ::-webkit-scrollbar-thumb {
        background: rgba(255,255,255,0.1);
        border-radius: 10px;
    }
</style>
""", unsafe_allow_html=True)

st.title("CareerRadar")
st.markdown('<p class="subtitle">Automated high-performance job discovery engine.</p>', unsafe_allow_html=True)

with st.sidebar:
    st.header("⚙️ Scrape Settings")
    
    strategy = st.selectbox("Strategy", ["Default", "Smart Fresher Hunt"], index=0, help="Smart Fresher Hunt will automatically test multiple keyword combinations to find hidden fresher jobs.")
    
    search_term = st.text_input("Search Term", value="software engineer fresher OR junior developer")
    
    col1, col2 = st.columns(2)
    with col1:
        location = st.text_input("Location", value="India")
    with col2:
        country_indeed = st.text_input("Country (Indeed)", value="India")
    
    col3, col4 = st.columns(2)
    with col3:
        results_wanted = st.number_input("Results Wanted", min_value=1, max_value=1000, value=50)
    with col4:
        hours_old = st.number_input("Hours Old", min_value=1, max_value=720, value=72)
        
    col5, col6 = st.columns(2)
    with col5:
        job_type = st.selectbox("Job Type", ["any", "fulltime", "parttime", "internship", "contract"])
    with col6:
        is_remote = st.checkbox("Remote Only", value=False)
        
    all_sites = ["indeed", "linkedin", "glassdoor", "zip_recruiter", "google", "naukri", "internshala", "foundit", "shine", "timesjobs"]
    sites = st.multiselect("Job Sites", all_sites, default=["indeed", "linkedin"])
    
    start_scrape = st.button("🚀 Start Scraping", use_container_width=True)

# Main area
if start_scrape:
    if not sites:
        st.error("Please select at least one job site.")
    else:
        with st.status("Scraping jobs... this might take a few minutes.", expanded=True) as status:
            st.write(f"Targeting {len(sites)} sites for '{search_term}'...")
            try:
                if strategy == "Smart Fresher Hunt":
                    st.write("Running smart fresher hunt (combining multiple keywords)...")
                    smart_queries = build_smart_queries(search_term)
                    preferred_days = max(1, int(math.ceil(hours_old / 24))) if hours_old else 7
                    jobs = scrape_smart_fresher_jobs(
                        top_n_combinations=max(10, len(smart_queries)) if smart_queries else 10,
                        location=location,
                        site_rotation=sites,
                        search_combinations=smart_queries if smart_queries else None,
                        country_indeed=country_indeed,
                        results_wanted_per_combo=results_wanted,
                        preferred_days_old=preferred_days,
                        fallback_days_old=max(30, preferred_days + 7),
                        enforce_degree_filter=False,
                        verbose=0
                    )
                    if jobs.empty:
                        st.warning("Smart fresher combinations returned 0 results. Running fallback broad scrape with your query...")
                        jobs = scrape_jobs(
                            site_name=sites,
                            search_term=search_term,
                            location=location,
                            results_wanted=results_wanted,
                            hours_old=hours_old,
                            country_indeed=country_indeed,
                            job_type=job_type if job_type != "any" else None,
                            is_remote=is_remote,
                        )
                    else:
                        st.write("Formatting results...")
                        jobs = format_hunt_results(jobs)
                else:
                    st.write("Running default scrape...")
                    jobs = scrape_jobs(
                        site_name=sites,
                        search_term=search_term,
                        location=location,
                        results_wanted=results_wanted,
                        hours_old=hours_old,
                        country_indeed=country_indeed,
                        job_type=job_type if job_type != "any" else None,
                        is_remote=is_remote,
                    )
                status.update(label=f"Scrape complete! Found {len(jobs)} jobs.", state="complete", expanded=False)
                
                st.session_state["jobs_df"] = jobs
                st.session_state["last_scrape"] = datetime.now()
            except Exception as e:
                status.update(label=f"Scrape failed: {str(e)}", state="error", expanded=True)

if "jobs_df" in st.session_state and st.session_state["jobs_df"] is not None:
    df = st.session_state["jobs_df"]
    
    if df.empty:
        st.warning("No jobs found with the current criteria.")
    else:
        # Metrics Row
        m1, m2, m3 = st.columns(3)
        m1.metric("Total Jobs Found", len(df))
        m2.metric("Platforms Successfully Scraped", len(df['site'].unique()) if 'site' in df.columns else (len(df['found_on_platforms'].unique()) if 'found_on_platforms' in df.columns else 1))
        m3.metric("Last Updated", st.session_state['last_scrape'].strftime('%H:%M:%S'))

        st.divider()
        
        # Filters
        st.markdown("### 📊 Filter Results")
        
        f1, f2 = st.columns([3, 1])
        with f1:
            text_filter = st.text_input("🔎 Quick Filter (Search across all columns):", placeholder="e.g. Python, remote, bangalore")
        with f2:
            st.write("") # spacing
            st.write("") # spacing
            fresher_only = st.checkbox("🎓 Fresher Roles Only", help="Strictly filter titles and descriptions for fresher/entry-level keywords.")
            
        display_df = df.copy()
        
        if text_filter:
            mask = display_df.astype(str).apply(lambda x: x.str.contains(text_filter, case=False, na=False)).any(axis=1)
            display_df = display_df[mask]
            
        if fresher_only:
            text_columns = [col for col in ("title", "description", "experience_range", "skills", "job_title", "description_full") if col in display_df.columns]
            if text_columns:
                fresher_pattern = r"intern|junior|fresher|entry|associate|trainee|graduate|new grad|0-1|0-2"
                searchable = display_df[text_columns].fillna("").astype(str).agg(" ".join, axis=1)
                display_df = display_df[searchable.str.contains(fresher_pattern, case=False, na=False)]
                
        # Make job urls clickable if they exist
        if 'job_url' in display_df.columns:
            st.dataframe(
                display_df,
                use_container_width=True,
                height=600,
                column_config={
                    "job_url": st.column_config.LinkColumn("Job Link"),
                    "description": st.column_config.TextColumn("Description", width="large")
                }
            )
        else:
            st.dataframe(display_df, use_container_width=True, height=600)
        
        # Download
        csv = display_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="⬇️ Download Filtered Results as CSV",
            data=csv,
            file_name="careerradar_results.csv",
            mime="text/csv",
            type="primary",
            use_container_width=True
        )
else:
    st.info("👈 Configure your settings in the sidebar and click 'Start Scraping' to begin.")
