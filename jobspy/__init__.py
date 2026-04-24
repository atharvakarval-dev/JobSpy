from __future__ import annotations

import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Tuple

import pandas as pd

from jobspy.bayt import BaytScraper
from jobspy.bdjobs import BDJobs
from jobspy.glassdoor import Glassdoor
from jobspy.google import Google
from jobspy.indeed import Indeed
from jobspy.linkedin import LinkedIn
from jobspy.naukri import Naukri
from jobspy.internshala import Internshala
from jobspy.foundit import Foundit
from jobspy.shine import Shine
from jobspy.timesjobs import TimesJobs
from jobspy.model import JobType, Location, JobResponse, Country
from jobspy.model import SalarySource, ScraperInput, Site
from jobspy.exception import ScraperWarning
from jobspy.util import (
    set_logger_level,
    extract_salary,
    create_logger,
    get_enum_from_value,
    map_str_to_site,
    convert_to_annual,
    desired_order,
)
from jobspy.ziprecruiter import ZipRecruiter
from jobspy.fresher_filter import filter_fresher_jobs
from jobspy.smart_hunt import (
    SearchCombination,
    generate_search_combinations,
    scrape_smart_fresher_jobs,
    format_hunt_results,
    match_keywords,
)


# Default fresher-focused search queries
DEFAULT_FRESHER_QUERIES = [
    "Fresher SDE",
    "New Grad Software Engineer",
    "Junior SDE",
    "Entry Level Developer",
    "0 years experience software",
]


def scrape_fresher_jobs(
    site_name: str | list[str] | Site | list[Site] | None = None,
    search_term: str | None = None,
    google_search_term: str | None = None,
    location: str | None = None,
    distance: int | None = 50,
    is_remote: bool = False,
    job_type: str | None = None,
    easy_apply: bool | None = None,
    results_wanted: int = 15,
    country_indeed: str = "usa",
    proxies: list[str] | str | None = None,
    ca_cert: str | None = None,
    description_format: str = "markdown",
    linkedin_fetch_description: bool | None = False,
    linkedin_company_ids: list[int] | None = None,
    offset: int | None = 0,
    hours_old: int | None = None,
    enforce_annual_salary: bool = False,
    verbose: int = 0,
    user_agent: str | None = None,
    delay_between_requests_ms: int | tuple[int, int] = 1000,
    linkedin_session_cookie: str | None = None,
    country: str | None = None,
    verbose_filter: bool = False,
    **kwargs,
) -> pd.DataFrame:
    """
    Scrapes job data and filters for fresher-level SDE roles.
    
    This is a wrapper around scrape_jobs() that applies additional filtering
    to target entry-level positions suitable for fresh graduates.
    
    Args:
        site_name: Job board site(s) to scrape
        search_term: Search query. Defaults to fresher-focused query if not provided.
        google_search_term: Custom Google search term
        location: Job location
        distance: Search radius in miles
        is_remote: Filter for remote jobs only
        job_type: Type of job (fulltime, parttime, etc.)
        easy_apply: Filter for easy-apply jobs only
        results_wanted: Number of results desired per site
        country_indeed: Country for Indeed searches
        proxies: Proxy configuration
        ca_cert: CA certificate for SSL verification
        description_format: Format for job descriptions (markdown, html, plain)
        linkedin_fetch_description: Whether to fetch full descriptions from LinkedIn
        linkedin_company_ids: Specific LinkedIn company IDs to search
        offset: Offset for pagination
        hours_old: Only return jobs posted within this many hours
        enforce_annual_salary: Convert all salaries to annual
        verbose: Logging verbosity (0, 1, or 2)
        user_agent: Custom User-Agent string
        delay_between_requests_ms: Delay between requests in milliseconds
        linkedin_session_cookie: LinkedIn session cookie for authenticated requests
        country: Country for location scoping (e.g., "India")
        verbose_filter: If True, logs which jobs were dropped and why
        **kwargs: Additional arguments passed to scrape_jobs
        
    Returns:
        DataFrame with fresher-relevant jobs, including fresher_signals
        and fresher_score columns
    """
    # Default to fresher-focused search term if not provided
    if search_term is None:
        search_term = "Fresher SDE OR New Grad Software Engineer OR Entry Level Developer"
    
    # Call the main scrape_jobs function
    jobs_df = scrape_jobs(
        site_name=site_name,
        search_term=search_term,
        google_search_term=google_search_term,
        location=location,
        distance=distance,
        is_remote=is_remote,
        job_type=job_type,
        easy_apply=easy_apply,
        results_wanted=results_wanted,
        country_indeed=country_indeed,
        proxies=proxies,
        ca_cert=ca_cert,
        description_format=description_format,
        linkedin_fetch_description=linkedin_fetch_description,
        linkedin_company_ids=linkedin_company_ids,
        offset=offset,
        hours_old=hours_old,
        enforce_annual_salary=enforce_annual_salary,
        verbose=verbose,
        user_agent=user_agent,
        delay_between_requests_ms=delay_between_requests_ms,
        linkedin_session_cookie=linkedin_session_cookie,
        country=country,
        **kwargs,
    )
    
    # Apply fresher filtering
    return filter_fresher_jobs(jobs_df, verbose=verbose_filter)


# Update the SCRAPER_MAPPING dictionary in the scrape_jobs function

def scrape_jobs(
    site_name: str | list[str] | Site | list[Site] | None = None,
    search_term: str | None = None,
    google_search_term: str | None = None,
    location: str | None = None,
    distance: int | None = 50,
    is_remote: bool = False,
    job_type: str | None = None,
    easy_apply: bool | None = None,
    results_wanted: int = 15,
    country_indeed: str = "usa",
    proxies: list[str] | str | None = None,
    ca_cert: str | None = None,
    description_format: str = "markdown",
    linkedin_fetch_description: bool | None = False,
    linkedin_company_ids: list[int] | None = None,
    offset: int | None = 0,
    hours_old: int = None,
    enforce_annual_salary: bool = False,
    verbose: int = 0,
    user_agent: str = None,
    delay_between_requests_ms: int | tuple[int, int] = 1000,
    linkedin_session_cookie: str | None = None,
    country: str | None = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Scrapes job data from job boards concurrently
    :return: Pandas DataFrame containing job data
    """
    SCRAPER_MAPPING = {
        Site.LINKEDIN: LinkedIn,
        Site.INDEED: Indeed,
        Site.ZIP_RECRUITER: ZipRecruiter,
        Site.GLASSDOOR: Glassdoor,
        Site.GOOGLE: Google,
        Site.BAYT: BaytScraper,
        Site.NAUKRI: Naukri,
        Site.BDJOBS: BDJobs,
        Site.INTERNSHALA: Internshala,
        Site.FOUNDIT: Foundit,
        Site.SHINE: Shine,
        Site.TIMESJOBS: TimesJobs,
    }
    set_logger_level(verbose)
    job_type = get_enum_from_value(job_type) if job_type else None

    def get_site_type():
        site_types = list(Site)
        if isinstance(site_name, str):
            site_types = [map_str_to_site(site_name)]
        elif isinstance(site_name, Site):
            site_types = [site_name]
        elif isinstance(site_name, list):
            site_types = [
                map_str_to_site(site) if isinstance(site, str) else site
                for site in site_name
            ]
        return site_types

    country_enum = Country.from_string(country_indeed)

    # P6: India country scoping
    site_types = get_site_type()
    processed_search_term = search_term
    processed_google_search_term = google_search_term
    if country == "India":
        country_indeed = "India"
        if location and "India" not in location:
            location = f"{location}, India"
        if google_search_term and "in India" not in google_search_term:
            processed_google_search_term = f"{google_search_term} in India"
        # Add naukri to sites if not already included
        if Site.NAUKRI not in site_types:
            site_types.append(Site.NAUKRI)

    scraper_input = ScraperInput(
        site_type=site_types,
        country=country_enum,
        search_term=processed_search_term,
        google_search_term=processed_google_search_term,
        location=location,
        distance=distance,
        is_remote=is_remote,
        job_type=job_type,
        easy_apply=easy_apply,
        description_format=description_format,
        linkedin_fetch_description=linkedin_fetch_description,
        results_wanted=results_wanted,
        linkedin_company_ids=linkedin_company_ids,
        offset=offset,
        hours_old=hours_old,
        delay_between_requests_ms=delay_between_requests_ms,
    )

    def scrape_site(site: Site) -> Tuple[str, JobResponse]:
        scraper_class = SCRAPER_MAPPING[site]
        # P4: Pass linkedin_session_cookie only to LinkedIn
        extra_args = {}
        if site == Site.LINKEDIN and linkedin_session_cookie:
            extra_args["linkedin_session_cookie"] = linkedin_session_cookie
        scraper = scraper_class(proxies=proxies, ca_cert=ca_cert, user_agent=user_agent, **extra_args)
        scraped_data: JobResponse = scraper.scrape(scraper_input)
        cap_name = site.value.capitalize()
        site_name = "ZipRecruiter" if cap_name == "Zip_recruiter" else cap_name
        site_name = "LinkedIn" if cap_name == "Linkedin" else cap_name
        create_logger(site_name).info(f"finished scraping")
        return site.value, scraped_data

    site_to_jobs_dict = {}
    scrape_metadata = {}

    def worker(site):
        site_val, scraped_info = scrape_site(site)
        return site_val, scraped_info

    with ThreadPoolExecutor() as executor:
        future_to_site = {
            executor.submit(worker, site): site for site in scraper_input.site_type
        }

        for future in as_completed(future_to_site):
            target_site = future_to_site[future]
            site_value = target_site.value
            try:
                site_value, scraped_data = future.result()
            except Exception as exc:
                scraped_data = JobResponse(jobs=[])
                site_to_jobs_dict[site_value] = scraped_data
                scrape_metadata[site_value] = {
                    "requested": scraper_input.results_wanted,
                    "returned": 0,
                    "status": "error",
                    "error": str(exc),
                }
                warnings.warn(
                    ScraperWarning(
                        f"[{site_value}] Scrape failed and was skipped: {exc}"
                    )
                )
                continue

            site_to_jobs_dict[site_value] = scraped_data
            # P1: Track metadata for silent failure detection
            returned_count = len(scraped_data.jobs)
            status = "ok" if returned_count >= scraper_input.results_wanted * 0.5 else "rate_limited"
            scrape_metadata[site_value] = {
                "requested": scraper_input.results_wanted,
                "returned": returned_count,
                "status": status,
            }
            # Emit warning if results are significantly lower than requested
            if returned_count < scraper_input.results_wanted * 0.5:
                warnings.warn(
                    ScraperWarning(
                        f"[{site_value}] Only {returned_count}/{scraper_input.results_wanted} results retrieved. "
                        f"Likely rate-limited. Add proxies or increase delay."
                    )
                )

    jobs_dfs: list[pd.DataFrame] = []

    for site, job_response in site_to_jobs_dict.items():
        for job in job_response.jobs:
            job_data = job.dict()
            job_url = job_data["job_url"]
            job_data["site"] = site
            job_data["company"] = job_data["company_name"]
            job_data["job_type"] = (
                ", ".join(job_type.value[0] for job_type in job_data["job_type"])
                if job_data["job_type"]
                else None
            )
            job_data["emails"] = (
                ", ".join(job_data["emails"]) if job_data["emails"] else None
            )
            if job_data["location"]:
                job_data["location"] = Location(
                    **job_data["location"]
                ).display_location()

            # Handle compensation
            compensation_obj = job_data.get("compensation")
            if compensation_obj and isinstance(compensation_obj, dict):
                job_data["interval"] = (
                    compensation_obj.get("interval").value
                    if compensation_obj.get("interval")
                    else None
                )
                job_data["min_amount"] = compensation_obj.get("min_amount")
                job_data["max_amount"] = compensation_obj.get("max_amount")
                job_data["currency"] = compensation_obj.get("currency", "USD")
                job_data["salary_source"] = SalarySource.DIRECT_DATA.value
                if enforce_annual_salary and (
                    job_data["interval"]
                    and job_data["interval"] != "yearly"
                    and job_data["min_amount"]
                    and job_data["max_amount"]
                ):
                    convert_to_annual(job_data)
            else:
                if country_enum == Country.USA:
                    (
                        job_data["interval"],
                        job_data["min_amount"],
                        job_data["max_amount"],
                        job_data["currency"],
                    ) = extract_salary(
                        job_data["description"],
                        enforce_annual_salary=enforce_annual_salary,
                    )
                    job_data["salary_source"] = SalarySource.DESCRIPTION.value

            job_data["salary_source"] = (
                job_data["salary_source"]
                if "min_amount" in job_data and job_data["min_amount"]
                else None
            )

            #naukri-specific fields
            job_data["skills"] = (
                ", ".join(job_data["skills"]) if job_data["skills"] else None
            )
            job_data["experience_range"] = job_data.get("experience_range")
            job_data["company_rating"] = job_data.get("company_rating")
            job_data["company_reviews_count"] = job_data.get("company_reviews_count")
            job_data["vacancy_count"] = job_data.get("vacancy_count")
            job_data["work_from_home_type"] = job_data.get("work_from_home_type")

            # internshala-specific fields
            job_data["is_internship"] = job_data.get("is_internship")
            job_data["stipend"] = job_data.get("stipend")
            job_data["apply_by"] = job_data.get("apply_by")

            job_df = pd.DataFrame([job_data])
            jobs_dfs.append(job_df)

    if jobs_dfs:
        filtered_dfs = [df.dropna(axis=1, how="all") for df in jobs_dfs]
        jobs_df = pd.concat(filtered_dfs, ignore_index=True)
        for column in desired_order:
            if column not in jobs_df.columns:
                jobs_df[column] = None  
        jobs_df = jobs_df[desired_order]
        jobs_df = jobs_df.sort_values(by=["site", "date_posted"], ascending=[True, False]).reset_index(drop=True)
        jobs_df.attrs["scrape_metadata"] = scrape_metadata
        return jobs_df
    else:
        empty_df = pd.DataFrame()
        empty_df.attrs["scrape_metadata"] = scrape_metadata
        return empty_df


__all__ = [
    "scrape_jobs",
    "scrape_fresher_jobs",
    "scrape_smart_fresher_jobs",
    "generate_search_combinations",
    "format_hunt_results",
    "match_keywords",
    "SearchCombination",
    "BDJobs",
]
