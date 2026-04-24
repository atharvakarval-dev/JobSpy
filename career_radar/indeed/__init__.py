from __future__ import annotations

import json
import math
import random
import time
from datetime import datetime
from typing import Any, Tuple

from career_radar.indeed.constant import job_search_query, api_headers
from career_radar.indeed.util import is_job_remote, get_compensation, get_job_type
from career_radar.exception import IndeedException
from career_radar.model import (
    Scraper,
    ScraperInput,
    Site,
    JobPost,
    Location,
    JobResponse,
    JobType,
    DescriptionFormat,
)
from career_radar.util import (
    extract_emails_from_text,
    markdown_converter,
    plain_converter,
    create_session,
    create_logger,
)

log = create_logger("Indeed")


class Indeed(Scraper):
    transient_statuses = {408, 425, 429, 500, 502, 503, 504, 520, 522, 524}

    def __init__(
        self, proxies: list[str] | str | None = None, ca_cert: str | None = None, user_agent: str | None = None
    ):
        """
        Initializes IndeedScraper with the Indeed API url
        """
        super().__init__(Site.INDEED, proxies=proxies)

        self.session = create_session(
            proxies=self.proxies, ca_cert=ca_cert, is_tls=False, has_retry=True, delay=2, user_agent=user_agent
        )
        self.scraper_input = None
        self.jobs_per_page = 100
        self.max_pages = 40
        self.max_retries = 4
        self.seen_urls = set()
        self.headers = None
        self.api_country_code = None
        self.base_url = None
        self.api_url = "https://apis.indeed.com/graphql"

    def scrape(self, scraper_input: ScraperInput) -> JobResponse:
        """
        Scrapes Indeed for jobs with scraper_input criteria
        :param scraper_input:
        :return: job_response
        """
        self.scraper_input = scraper_input
        domain, self.api_country_code = self.scraper_input.country.indeed_domain_value
        self.base_url = f"https://{domain}.indeed.com"
        self.headers = api_headers.copy()
        self.headers["indeed-co"] = self.api_country_code
        job_list = []
        page = 1

        cursor = None
        last_cursor = None
        empty_page_streak = 0
        max_results = scraper_input.results_wanted + scraper_input.offset

        while len(self.seen_urls) < max_results and page <= self.max_pages:
            log.info(
                f"search page: {page} / {math.ceil(scraper_input.results_wanted / self.jobs_per_page)}"
            )
            jobs, cursor = self._scrape_page(cursor)
            if not jobs:
                log.info(f"found no jobs on page: {page}")
                empty_page_streak += 1
                if empty_page_streak >= 2:
                    break
            else:
                empty_page_streak = 0
            job_list += jobs

            if not cursor or cursor == last_cursor:
                break
            last_cursor = cursor
            page += 1
            if len(self.seen_urls) < max_results:
                time.sleep(self._get_delay_seconds())

        return JobResponse(
            jobs=job_list[
                scraper_input.offset : scraper_input.offset
                + scraper_input.results_wanted
            ]
        )

    def _scrape_page(self, cursor: str | None) -> Tuple[list[JobPost], str | None]:
        """
        Scrapes a page of Indeed for jobs with scraper_input criteria
        :param cursor:
        :return: jobs found on page, next page cursor
        """
        jobs = []
        new_cursor = None
        filters = self._build_filters()
        search_term = self.scraper_input.search_term or ""
        query = job_search_query.format(
            what=(f'what: {json.dumps(search_term)}' if search_term else ""),
            location=(
                f"location: {{where: {json.dumps(self.scraper_input.location)}, radius: {self.scraper_input.distance}, radiusUnit: MILES}}"
                if self.scraper_input.location
                else ""
            ),
            dateOnIndeed=self.scraper_input.hours_old,
            cursor=f'cursor: "{cursor}"' if cursor else "",
            filters=filters,
        )
        payload = {
            "query": query,
        }
        data = self._post_graphql(payload)
        if not data:
            return jobs, new_cursor

        job_search = data.get("data", {}).get("jobSearch", {}) if isinstance(data, dict) else {}
        jobs = job_search.get("results") or []
        page_info = job_search.get("pageInfo") or {}
        new_cursor = page_info.get("nextCursor")

        job_list = []
        for job in jobs:
            if not isinstance(job, dict):
                continue
            processed_job = self._process_job(job.get("job") or {})
            if processed_job is not None:
                job_list.append(processed_job)

        return job_list, new_cursor

    def _post_graphql(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        """Posts the Indeed GraphQL request with retry/backoff for transient failures."""
        timeout = max(10, int(self.scraper_input.request_timeout or 10))
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.post(
                    self.api_url,
                    headers=self.headers,
                    json=payload,
                    timeout=timeout,
                )
            except Exception as exc:
                if attempt == self.max_retries:
                    log.error("Indeed request failed after retries: %s", exc)
                    return None
                self._sleep_backoff(attempt, "request exception")
                continue

            if response.status_code in self.transient_statuses:
                if attempt == self.max_retries:
                    log.warning("Indeed transient error persisted (%s)", response.status_code)
                    return None
                self._sleep_backoff(attempt, f"status={response.status_code}")
                continue

            if not response.ok:
                log.warning("Indeed non-success status code: %s", response.status_code)
                return None

            try:
                data = response.json()
            except ValueError:
                log.warning("Indeed returned non-JSON response.")
                return None

            if isinstance(data, dict) and data.get("errors") and not data.get("data"):
                log.warning("Indeed GraphQL returned errors without data.")
                return None
            return data

        raise IndeedException("Unexpected retry loop termination for Indeed GraphQL")

    def _build_filters(self):
        """
        Builds the filters dict for job type/is_remote. If hours_old is provided, composite filter for job_type/is_remote is not possible.
        IndeedApply: filters: { keyword: { field: "indeedApplyScope", keys: ["DESKTOP"] } }
        """
        filters_str = ""
        if self.scraper_input.hours_old:
            filters_str = """
            filters: {{
                date: {{
                  field: "dateOnIndeed",
                  start: "{start}h"
                }}
            }}
            """.format(
                start=self.scraper_input.hours_old
            )
        elif self.scraper_input.easy_apply:
            filters_str = """
            filters: {
                keyword: {
                  field: "indeedApplyScope",
                  keys: ["DESKTOP"]
                }
            }
            """
        elif self.scraper_input.job_type or self.scraper_input.is_remote:
            job_type_key_mapping = {
                JobType.FULL_TIME: "CF3CP",
                JobType.PART_TIME: "75GKK",
                JobType.CONTRACT: "NJXCK",
                JobType.INTERNSHIP: "VDTG7",
            }

            keys = []
            if self.scraper_input.job_type:
                key = job_type_key_mapping[self.scraper_input.job_type]
                keys.append(key)

            if self.scraper_input.is_remote:
                keys.append("DSQF7")

            if keys:
                keys_str = '", "'.join(keys)
                filters_str = f"""
                filters: {{
                  composite: {{
                    filters: [{{
                      keyword: {{
                        field: "attributes",
                        keys: ["{keys_str}"]
                      }}
                    }}]
                  }}
                }}
                """
        return filters_str

    def _sleep_backoff(self, attempt: int, reason: str) -> None:
        """Backoff helper with jitter for transient request failures."""
        base = [1.5, 4.0, 8.0, 16.0]
        idx = min(attempt - 1, len(base) - 1)
        wait_for = base[idx] + random.uniform(0.2, 1.0)
        log.warning("Indeed retrying in %.1fs (%s)", wait_for, reason)
        time.sleep(wait_for)

    def _get_delay_seconds(self) -> float:
        """Uses configured scrape delay to avoid request bursts."""
        delay_config = self.scraper_input.delay_between_requests_ms
        if isinstance(delay_config, tuple):
            min_ms, max_ms = delay_config
            return max(0.5, random.uniform(min_ms, max_ms) / 1000.0)
        return max(0.5, float(delay_config) / 1000.0)

    def _process_job(self, job: dict) -> JobPost | None:
        """
        Parses the job dict into JobPost model
        :param job: dict to parse
        :return: JobPost if it's a new job
        """
        job_key = str(job.get("key") or "").strip()
        if not job_key:
            return None
        job_url = f"{self.base_url}/viewjob?jk={job_key}"
        if job_url in self.seen_urls:
            return None
        self.seen_urls.add(job_url)

        description = ((job.get("description") or {}).get("html") or "").strip()
        if self.scraper_input.description_format == DescriptionFormat.MARKDOWN:
            description = markdown_converter(description)
        elif self.scraper_input.description_format == DescriptionFormat.PLAIN:
            description = plain_converter(description)

        attributes = job.get("attributes") or []
        try:
            job_type = get_job_type(attributes) if attributes else None
        except Exception:
            job_type = None

        date_posted = None
        timestamp_raw = job.get("datePublished")
        if timestamp_raw:
            try:
                timestamp_seconds = float(timestamp_raw) / 1000
                date_posted = datetime.fromtimestamp(timestamp_seconds).date()
            except Exception:
                date_posted = None

        employer_data = job.get("employer") or {}
        dossier = employer_data.get("dossier") or {}
        employer_details = dossier.get("employerDetails") or {}
        rel_url = employer_data.get("relativeCompanyPageUrl")

        company_industry = employer_details.get("industry")
        if isinstance(company_industry, str):
            company_industry = company_industry.replace("Iv1", "").replace("_", " ").title().strip()

        location_data = job.get("location") or {}
        location_country = location_data.get("countryCode")
        location_city = location_data.get("city")
        location_state = location_data.get("admin1Code")

        compensation = None
        try:
            if job.get("compensation"):
                compensation = get_compensation(job["compensation"])
        except Exception:
            compensation = None

        recruit_data = job.get("recruit") or {}
        location_formatted = ((location_data.get("formatted") or {}).get("long") or "").lower()
        fallback_remote = "remote" in location_formatted or "work from home" in location_formatted
        try:
            remote_flag = is_job_remote(job, str(description or ""))
        except Exception:
            remote_flag = fallback_remote

        addresses = employer_details.get("addresses")
        if isinstance(addresses, list) and addresses:
            company_address = addresses[0]
        else:
            company_address = None

        company_images = dossier.get("images") or {}
        company_links = dossier.get("links") or {}

        return JobPost(
            id=f"in-{job_key}",
            title=job.get("title") or "N/A",
            description=description,
            company_name=employer_data.get("name"),
            company_url=(f"{self.base_url}{rel_url}" if rel_url else None),
            company_url_direct=company_links.get("corporateWebsite"),
            location=Location(
                city=location_city,
                state=location_state,
                country=location_country,
            ),
            job_type=job_type,
            compensation=compensation,
            date_posted=date_posted,
            job_url=job_url,
            job_url_direct=recruit_data.get("viewJobUrl"),
            emails=extract_emails_from_text(description) if description else None,
            is_remote=remote_flag,
            company_addresses=company_address,
            company_industry=company_industry,
            company_num_employees=employer_details.get("employeesLocalizedLabel"),
            company_revenue=employer_details.get("revenueLocalizedLabel"),
            company_description=employer_details.get("briefDescription"),
            company_logo=company_images.get("squareLogoUrl"),
        )
