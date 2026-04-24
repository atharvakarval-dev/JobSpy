# Foundit (formerly Monster India) Scraper for CareerRadar
# =====================================================
# Usage:
#   from career_radar import scrape_jobs
#   jobs = scrape_jobs(
#       site_name="foundit",
#       search_term="software engineer",
#       location="Pune",
#       results_wanted=20,
#       hours_old=72,
#   )
#   print(jobs)
#
# Foundit uses a middleware JSON API at:
#   https://www.foundit.in/middleware/jobsearch/v3/search
# The API is WAF-protected (Cloudflare), so we use TLS client sessions.

from __future__ import annotations

import math
import random
import time
from datetime import datetime, date, timedelta
from typing import Optional

import regex as re

from career_radar.exception import FounditException
from career_radar.model import (
    JobPost,
    Location,
    JobResponse,
    Country,
    Compensation,
    CompensationInterval,
    DescriptionFormat,
    Scraper,
    ScraperInput,
    Site,
    JobType,
)
from career_radar.util import (
    extract_emails_from_text,
    markdown_converter,
    create_session,
    create_logger,
)

log = create_logger("Foundit")


class Foundit(Scraper):
    base_url = "https://www.foundit.in"
    api_url = "https://www.foundit.in/middleware/jobsearch/v3/search"
    delay = 3
    band_delay = 4
    jobs_per_page = 15

    def __init__(
        self,
        proxies: list[str] | str | None = None,
        ca_cert: str | None = None,
        user_agent: str | None = None,
    ):
        """
        Initializes FounditScraper with TLS client session for WAF bypass
        """
        super().__init__(Site.FOUNDIT, proxies=proxies, ca_cert=ca_cert)
        self.session = create_session(
            proxies=self.proxies,
            ca_cert=ca_cert,
            is_tls=True,  # TLS fingerprinting to bypass Cloudflare
            has_retry=False,
            delay=5,
        )
        self.session.headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-US,en;q=0.9",
            "content-type": "application/json",
            "origin": "https://www.foundit.in",
            "referer": "https://www.foundit.in/srp/results",
            "user-agent": user_agent
            or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }
        self.scraper_input = None
        self.country = "India"
        log.info("Foundit scraper initialized")

    def scrape(self, scraper_input: ScraperInput) -> JobResponse:
        """
        Scrapes Foundit API for jobs matching the scraper_input criteria
        :param scraper_input: ScraperInput with search params
        :return: JobResponse containing matched jobs
        """
        self.scraper_input = scraper_input
        job_list: list[JobPost] = []
        seen_ids: set[str] = set()
        page = 1
        request_count = 0

        continue_search = (
            lambda: len(job_list) < scraper_input.results_wanted and page <= 30
        )

        while continue_search():
            request_count += 1
            log.info(
                f"Scraping page {request_count} / {math.ceil(scraper_input.results_wanted / self.jobs_per_page)} "
                f"for search term: {scraper_input.search_term}"
            )

            params = self._build_params(scraper_input, page)

            try:
                log.debug(f"Sending request to {self.api_url} with params: {params}")
                response = self.session.get(
                    self.api_url, params=params
                )
                if response.status_code not in range(200, 400):
                    err = f"Foundit API response status code {response.status_code}"
                    log.error(err)
                    # Try alternate scraping approach via HTML
                    html_jobs = self._scrape_html_fallback(scraper_input, page)
                    if html_jobs:
                        for job in html_jobs:
                            if job.id not in seen_ids:
                                seen_ids.add(job.id)
                                job_list.append(job)
                        page += 1
                        continue
                    return JobResponse(jobs=job_list)

                data = response.json()
                job_details = data.get("jobSearchResponse", {}).get("data", [])
                if not job_details:
                    # Try alternate response structure
                    job_details = data.get("data", [])
                log.info(f"Received {len(job_details)} job entries from API")

                if not job_details:
                    log.warning("No job details found in API response")
                    break

            except Exception as e:
                log.error(f"Foundit API request failed: {str(e)}")
                return JobResponse(jobs=job_list)

            for job in job_details:
                job_id = str(
                    job.get("jobId") or job.get("id") or job.get("groupId", "")
                )
                if not job_id or job_id in seen_ids:
                    continue
                seen_ids.add(job_id)

                try:
                    job_post = self._process_job(job, job_id)
                    if job_post:
                        job_list.append(job_post)
                        log.info(f"Added job: {job_post.title} (ID: {job_id})")
                    if not continue_search():
                        break
                except Exception as e:
                    log.warning(f"Error processing job ID {job_id}: {str(e)}")
                    continue

            if continue_search():
                time.sleep(random.uniform(self.delay, self.delay + self.band_delay))
                page += 1

        job_list = job_list[: scraper_input.results_wanted]
        log.info(f"Scraping completed. Total jobs collected: {len(job_list)}")
        return JobResponse(jobs=job_list)

    def _build_params(self, scraper_input: ScraperInput, page: int) -> dict:
        """
        Builds API query params for Foundit search
        """
        params = {
            "query": scraper_input.search_term or "",
            "location": scraper_input.location or "",
            "pageNo": page,
            "limit": self.jobs_per_page,
            "sort": "1",  # Sort by relevance
        }

        # Job type mapping
        if scraper_input.job_type:
            type_map = {
                JobType.FULL_TIME: "Full Time",
                JobType.PART_TIME: "Part Time",
                JobType.CONTRACT: "Contract",
                JobType.INTERNSHIP: "Internship",
            }
            jt = type_map.get(scraper_input.job_type)
            if jt:
                params["jobType"] = jt

        # Hours old filter
        if scraper_input.hours_old:
            days = scraper_input.hours_old // 24
            if days <= 1:
                params["postedDate"] = "1"
            elif days <= 3:
                params["postedDate"] = "3"
            elif days <= 7:
                params["postedDate"] = "7"
            elif days <= 15:
                params["postedDate"] = "15"
            else:
                params["postedDate"] = "30"

        if scraper_input.is_remote:
            params["workMode"] = "Work from Home"

        return {k: v for k, v in params.items() if v is not None}

    def _process_job(self, job: dict, job_id: str) -> Optional[JobPost]:
        """
        Processes a single job from Foundit API response into a JobPost
        """
        title = job.get("title") or job.get("designation") or "N/A"
        company = job.get("companyName") or job.get("company", "N/A")

        # Location
        location = self._parse_location(job)

        # Compensation
        compensation = self._parse_compensation(job)

        # Date posted
        date_posted = self._parse_date(job)

        # Job URL
        seo_url = job.get("seoJDUrl") or job.get("jdUrl") or job.get("jobUrl", "")
        if seo_url and not seo_url.startswith("http"):
            job_url = f"{self.base_url}{seo_url}"
        elif seo_url:
            job_url = seo_url
        else:
            job_url = f"{self.base_url}/job/{job_id}"

        # Description
        description = job.get("jobDescription") or job.get("jdSnippet")
        if (
            description
            and self.scraper_input
            and self.scraper_input.description_format == DescriptionFormat.MARKDOWN
        ):
            description = markdown_converter(description)

        # Skills
        skills_raw = job.get("skills") or job.get("keySkills") or []
        if isinstance(skills_raw, str):
            skills = [s.strip() for s in skills_raw.split(",") if s.strip()]
        elif isinstance(skills_raw, list):
            skills = [
                (s.get("name") if isinstance(s, dict) else str(s))
                for s in skills_raw
            ]
            skills = [s for s in skills if s]
        else:
            skills = None

        # Experience
        exp_min = job.get("minimumExperience") or job.get("expMin")
        exp_max = job.get("maximumExperience") or job.get("expMax")
        experience_range = None
        if exp_min is not None or exp_max is not None:
            experience_range = f"{exp_min or 0}-{exp_max or '?'} years"

        # Job type
        job_type_str = job.get("jobType") or job.get("type", "")
        job_type = self._parse_job_type(job_type_str)

        # Remote
        work_mode = job.get("workMode") or job.get("workType", "")
        is_remote = any(
            kw in work_mode.lower()
            for kw in ["remote", "work from home", "wfh"]
        ) if work_mode else False

        # Company logo
        company_logo = job.get("companyLogo") or job.get("logoUrl")

        job_post = JobPost(
            id=f"fi-{job_id}",
            title=title,
            company_name=company,
            job_url=job_url,
            location=location,
            date_posted=date_posted,
            compensation=compensation,
            job_type=job_type,
            is_remote=is_remote,
            description=description,
            emails=extract_emails_from_text(description or ""),
            company_logo=company_logo,
            skills=skills,
            experience_range=experience_range,
        )
        log.debug(f"Processed job: {title} at {company}")
        return job_post

    def _parse_location(self, job: dict) -> Location:
        """
        Parses location from Foundit API response
        """
        loc_list = job.get("locations") or job.get("location")
        if isinstance(loc_list, list) and loc_list:
            loc = loc_list[0] if isinstance(loc_list[0], str) else loc_list[0].get("name", "")
        elif isinstance(loc_list, str):
            loc = loc_list
        else:
            loc = job.get("city", "")

        parts = [p.strip() for p in loc.split(",")] if loc else []
        city = parts[0] if parts else None
        state = parts[1] if len(parts) > 1 else None
        return Location(city=city, state=state, country=Country.INDIA)

    def _parse_compensation(self, job: dict) -> Optional[Compensation]:
        """
        Parses salary from Foundit API response
        """
        salary_text = job.get("salary") or job.get("salaryRange") or ""
        min_salary = job.get("salaryMin") or job.get("minimumSalary")
        max_salary = job.get("salaryMax") or job.get("maximumSalary")

        if min_salary and max_salary:
            try:
                return Compensation(
                    min_amount=int(float(min_salary)),
                    max_amount=int(float(max_salary)),
                    currency="INR",
                    interval=CompensationInterval.YEARLY,
                )
            except (ValueError, TypeError):
                pass

        if salary_text:
            # Try parsing "X - Y Lacs" format
            match = re.search(
                r"(\d+(?:\.\d+)?)\s*[-–]\s*(\d+(?:\.\d+)?)\s*(?:Lacs?|Lakh|LPA)",
                salary_text,
                re.IGNORECASE,
            )
            if match:
                min_val = float(match.group(1)) * 100000
                max_val = float(match.group(2)) * 100000
                return Compensation(
                    min_amount=int(min_val),
                    max_amount=int(max_val),
                    currency="INR",
                    interval=CompensationInterval.YEARLY,
                )

        return None

    def _parse_date(self, job: dict) -> Optional[date]:
        """
        Parses posting date from Foundit API response
        """
        date_str = job.get("postedDate") or job.get("createdDate") or job.get("modifiedDate")
        if not date_str:
            return None

        # Handle relative dates
        if isinstance(date_str, str):
            lower = date_str.lower()
            today = datetime.now()

            if "today" in lower or "just now" in lower:
                return today.date()

            day_match = re.search(r"(\d+)\s*day", lower)
            if day_match:
                return (today - timedelta(days=int(day_match.group(1)))).date()

            # Try ISO format
            for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S"):
                try:
                    return datetime.strptime(date_str.split("T")[0] if "T" in date_str else date_str, fmt.split("T")[0]).date()
                except ValueError:
                    continue

        # Handle timestamp
        if isinstance(date_str, (int, float)):
            try:
                return datetime.fromtimestamp(date_str / 1000).date()
            except (ValueError, OSError):
                pass

        return None

    def _parse_job_type(self, job_type_str: str) -> list[JobType] | None:
        """
        Maps Foundit job type string to JobType enum
        """
        if not job_type_str:
            return None

        mapping = {
            "full time": JobType.FULL_TIME,
            "fulltime": JobType.FULL_TIME,
            "part time": JobType.PART_TIME,
            "parttime": JobType.PART_TIME,
            "contract": JobType.CONTRACT,
            "internship": JobType.INTERNSHIP,
            "temporary": JobType.TEMPORARY,
        }

        jt = mapping.get(job_type_str.lower().strip())
        return [jt] if jt else None

    def _scrape_html_fallback(
        self, scraper_input: ScraperInput, page: int
    ) -> list[JobPost]:
        """
        Fallback HTML scraping if the JSON API is blocked.
        Scrapes the SRP (Search Results Page) directly.
        """
        try:
            from bs4 import BeautifulSoup

            keyword = (scraper_input.search_term or "").replace(" ", "-")
            location = (scraper_input.location or "").replace(" ", "-")
            url = f"{self.base_url}/srp/results?query={keyword}&locations={location}&page={page}"

            log.info(f"Attempting HTML fallback at {url}")
            response = self.session.get(url)
            if response.status_code not in range(200, 400):
                log.warning(f"HTML fallback also failed: {response.status_code}")
                return []

            soup = BeautifulSoup(response.text, "html.parser")
            jobs = []

            # Look for job cards in SRP
            cards = soup.select(".card-apply-content, .job-card, .srpResultCardContainer")
            for card in cards:
                try:
                    title_el = card.select_one(".job-title, .card-title, h3 a")
                    if not title_el:
                        continue

                    title = title_el.get_text(strip=True)
                    href = title_el.get("href", "")
                    job_url = href if href.startswith("http") else f"{self.base_url}{href}"

                    company_el = card.select_one(".company-name, .card-company")
                    company = company_el.get_text(strip=True) if company_el else None

                    loc_el = card.select_one(".loc, .card-location")
                    loc_text = loc_el.get_text(strip=True) if loc_el else ""
                    parts = loc_text.split(",")

                    job_id = re.search(r"(\d{6,})", href)
                    jid = job_id.group(1) if job_id else str(hash(title + (company or "")))

                    jobs.append(
                        JobPost(
                            id=f"fi-{jid}",
                            title=title,
                            company_name=company,
                            job_url=job_url,
                            location=Location(
                                city=parts[0].strip() if parts else None,
                                country=Country.INDIA,
                            ),
                        )
                    )
                except Exception as e:
                    log.warning(f"Error in HTML fallback card: {e}")
                    continue

            return jobs
        except Exception as e:
            log.warning(f"HTML fallback failed entirely: {e}")
            return []
