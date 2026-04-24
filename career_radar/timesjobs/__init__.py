# TimesJobs Scraper for CareerRadar
# ==============================
# Usage:
#   from career_radar import scrape_jobs
#   jobs = scrape_jobs(
#       site_name="timesjobs",
#       search_term="software engineer",
#       location="Pune",
#       results_wanted=20,
#   )
#   print(jobs)
#
# NOTE: TimesJobs has been rebuilt as "Cand" — a Next.js SPA.
# This scraper uses their server-side rendered search page and extracts
# data from the HTML/embedded JSON. The old HTML class selectors
# (clearfix job-bx wht-shd-bx) are no longer valid.

from __future__ import annotations

import json
import math
import random
import time
from datetime import datetime, date, timedelta
from typing import Optional

import regex as re
from bs4 import BeautifulSoup

from career_radar.exception import TimesJobsException
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

log = create_logger("TimesJobs")


class TimesJobs(Scraper):
    base_url = "https://www.timesjobs.com"
    delay = 2
    band_delay = 3
    jobs_per_page = 25

    def __init__(
        self,
        proxies: list[str] | str | None = None,
        ca_cert: str | None = None,
        user_agent: str | None = None,
    ):
        """
        Initializes TimesJobsScraper for scraping timesjobs.com
        """
        super().__init__(Site.TIMESJOBS, proxies=proxies, ca_cert=ca_cert)
        self.session = create_session(
            proxies=self.proxies,
            ca_cert=ca_cert,
            is_tls=True,
            has_retry=False,
            delay=5,
        )
        self.session.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "accept-language": "en-US,en;q=0.9",
            "user-agent": user_agent
            or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }
        self.scraper_input = None
        self.country = "India"
        log.info("TimesJobs scraper initialized")

    def scrape(self, scraper_input: ScraperInput) -> JobResponse:
        """
        Scrapes TimesJobs (Cand) for jobs matching the criteria
        """
        self.scraper_input = scraper_input
        job_list: list[JobPost] = []
        seen_ids: set[str] = set()
        page = 1
        request_count = 0

        continue_search = (
            lambda: len(job_list) < scraper_input.results_wanted and page <= 20
        )

        while continue_search():
            request_count += 1
            log.info(
                f"Scraping page {request_count} / {math.ceil(scraper_input.results_wanted / self.jobs_per_page)} "
                f"for search term: {scraper_input.search_term}"
            )

            url = self._build_url(scraper_input, page)

            try:
                log.debug(f"Sending request to {url}")
                response = self.session.get(url)
                if response.status_code not in range(200, 400):
                    err = f"TimesJobs response status code {response.status_code}"
                    log.error(err)
                    return JobResponse(jobs=job_list)

                html = response.text
                soup = BeautifulSoup(html, "html.parser")

                # Strategy 1: Try extracting from __NEXT_DATA__ (new Cand SPA)
                next_data_jobs = self._extract_next_data(soup)
                if next_data_jobs:
                    for job_post in next_data_jobs:
                        if job_post.id not in seen_ids:
                            seen_ids.add(job_post.id)
                            job_list.append(job_post)
                            if not continue_search():
                                break
                    if continue_search():
                        page += 1
                        time.sleep(random.uniform(self.delay, self.delay + self.band_delay))
                    continue

                # Strategy 2: Try the legacy HTML structure (in case of redirect to old site)
                legacy_cards = soup.select(
                    ".clearfix.job-bx.wht-shd-bx, .job-bx, "
                    ".srp-job-card, [class*='jobCard'], .job-card"
                )
                if legacy_cards:
                    for card in legacy_cards:
                        try:
                            job_post = self._process_legacy_card(card)
                            if job_post and job_post.id not in seen_ids:
                                seen_ids.add(job_post.id)
                                job_list.append(job_post)
                                if not continue_search():
                                    break
                        except Exception as e:
                            log.warning(f"Error processing legacy card: {e}")
                            continue
                else:
                    # Strategy 3: Try generic HTML card extraction
                    generic_cards = soup.select(
                        "a[href*='/job/'], article, [data-job-id], .card"
                    )
                    if not generic_cards:
                        log.warning("No job data found on page")
                        break

                    for card in generic_cards:
                        try:
                            job_post = self._process_generic_card(card)
                            if job_post and job_post.id not in seen_ids:
                                seen_ids.add(job_post.id)
                                job_list.append(job_post)
                                if not continue_search():
                                    break
                        except Exception as e:
                            log.warning(f"Error processing generic card: {e}")
                            continue

            except Exception as e:
                log.error(f"TimesJobs request failed: {str(e)}")
                return JobResponse(jobs=job_list)

            if continue_search():
                time.sleep(random.uniform(self.delay, self.delay + self.band_delay))
                page += 1

        job_list = job_list[: scraper_input.results_wanted]
        log.info(f"Scraping completed. Total jobs collected: {len(job_list)}")
        return JobResponse(jobs=job_list)

    def _build_url(self, scraper_input: ScraperInput, page: int) -> str:
        """
        Builds TimesJobs / Cand search URL
        """
        keyword = scraper_input.search_term or ""
        location = scraper_input.location or ""

        # Try the new Cand search URL format
        params = f"?searchType=personalizedSearch&from=submit&txtKeywords={keyword.replace(' ', '+')}&txtLocation={location.replace(' ', '+')}"

        if page > 1:
            params += f"&sequence={page}&startPage={page}"

        # Filter by hours old
        if scraper_input.hours_old:
            days = scraper_input.hours_old // 24
            if days <= 1:
                params += "&postedDate=1"
            elif days <= 3:
                params += "&postedDate=3"
            elif days <= 7:
                params += "&postedDate=7"
            elif days <= 15:
                params += "&postedDate=15"
            else:
                params += "&postedDate=30"

        return f"{self.base_url}/candidate/job-search.html{params}"

    def _extract_next_data(self, soup: BeautifulSoup) -> list[JobPost]:
        """
        Extracts job data from Next.js __NEXT_DATA__ script tag
        """
        jobs = []

        script = soup.select_one("script#__NEXT_DATA__")
        if not script or not script.string:
            # Also try the RSC payload format (React Server Components)
            return self._extract_from_rsc_payload(soup)

        try:
            data = json.loads(script.string)
            page_props = data.get("props", {}).get("pageProps", {})

            # Try common key patterns
            job_list = (
                page_props.get("jobs")
                or page_props.get("jobList")
                or page_props.get("searchResults", {}).get("jobs", [])
                or page_props.get("data", {}).get("jobs", [])
                or []
            )

            for job_data in job_list:
                job_post = self._parse_next_data_job(job_data)
                if job_post:
                    jobs.append(job_post)

        except (json.JSONDecodeError, AttributeError, KeyError) as e:
            log.warning(f"Error parsing __NEXT_DATA__: {e}")

        return jobs

    def _extract_from_rsc_payload(self, soup: BeautifulSoup) -> list[JobPost]:
        """
        Tries to extract job data from React Server Components payload.
        The new Cand site uses RSC streaming format.
        """
        jobs = []

        # RSC payloads are in script tags with self.__next_f.push format
        for script in soup.select("script"):
            if not script.string or "self.__next_f" not in script.string:
                continue

            # Try to extract JSON objects from the RSC payload
            json_matches = re.findall(r'\{[^{}]*"title"[^{}]*"company"[^{}]*\}', script.string)
            for match_str in json_matches:
                try:
                    job_data = json.loads(match_str)
                    if job_data.get("title"):
                        job_post = self._parse_next_data_job(job_data)
                        if job_post:
                            jobs.append(job_post)
                except json.JSONDecodeError:
                    continue

        return jobs

    def _parse_next_data_job(self, job: dict) -> Optional[JobPost]:
        """
        Parses a job from Next.js data into a JobPost
        """
        title = job.get("title") or job.get("jobTitle", "")
        if not title:
            return None

        company = job.get("company") or job.get("companyName", "")
        job_id = str(
            job.get("id") or job.get("jobId") or job.get("_id") or hash(title + company)
        )

        # Location
        loc = job.get("location") or job.get("city", "")
        if isinstance(loc, list):
            loc = ", ".join(loc)
        parts = [p.strip() for p in loc.split(",")] if isinstance(loc, str) else []
        location = Location(
            city=parts[0] if parts else None,
            state=parts[1] if len(parts) > 1 else None,
            country=Country.INDIA,
        )

        # URL
        slug = job.get("slug") or job.get("seoUrl") or job.get("url", "")
        if slug and not slug.startswith("http"):
            job_url = f"{self.base_url}/{slug.lstrip('/')}"
        elif slug:
            job_url = slug
        else:
            job_url = f"{self.base_url}/job/{job_id}"

        # Date
        date_posted = None
        date_str = job.get("postedDate") or job.get("createdAt") or job.get("datePosted")
        if date_str:
            date_posted = self._parse_date_str(date_str)

        # Salary
        compensation = self._parse_salary(job)

        # Skills
        skills_raw = job.get("skills") or job.get("keySkills") or job.get("tags", [])
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
        exp = job.get("experience") or job.get("experienceRange", "")
        experience_range = str(exp) if exp else None

        # Description
        description = job.get("description") or job.get("jobDescription", "")
        if (
            description
            and self.scraper_input
            and self.scraper_input.description_format == DescriptionFormat.MARKDOWN
        ):
            description = markdown_converter(description)

        return JobPost(
            id=f"tj-{job_id}",
            title=title,
            company_name=company or None,
            job_url=job_url,
            location=location,
            date_posted=date_posted,
            compensation=compensation,
            description=description,
            skills=skills if skills else None,
            experience_range=experience_range,
            emails=extract_emails_from_text(description or ""),
        )

    def _process_legacy_card(self, card) -> Optional[JobPost]:
        """
        Processes a legacy TimesJobs HTML card (pre-Cand redesign)
        """
        # Title
        title_elem = card.select_one("h2 a, .heading a, .job-title a")
        title = title_elem.get_text(strip=True) if title_elem else None
        if not title:
            return None

        href = title_elem.get("href", "") if title_elem else ""
        job_url = href if href.startswith("http") else f"{self.base_url}{href}"

        id_match = re.search(r"[/-](\d{6,})", href)
        job_id = id_match.group(1) if id_match else str(hash(title))

        # Company
        company_elem = card.select_one(
            ".joblist-comp-name, .company-name, h3.joblist-comp-name"
        )
        company = company_elem.get_text(strip=True) if company_elem else None

        # Location
        loc_elem = card.select_one(".location, .loc, [class*='location']")
        loc_text = loc_elem.get_text(strip=True) if loc_elem else ""
        # Clean up location text (remove icons/extra text)
        loc_text = re.sub(r"[^\w\s,]", "", loc_text).strip()
        parts = [p.strip() for p in loc_text.split(",")] if loc_text else []
        location = Location(
            city=parts[0] if parts else None,
            state=parts[1] if len(parts) > 1 else None,
            country=Country.INDIA,
        )

        # Experience
        exp_elem = card.select_one(
            ".exp, [class*='experience'], .job-experience"
        )
        experience_range = exp_elem.get_text(strip=True) if exp_elem else None

        # Salary
        salary_elem = card.select_one(
            ".sal, [class*='salary'], .job-salary"
        )
        salary_text = salary_elem.get_text(strip=True) if salary_elem else ""
        compensation = self._parse_salary_text(salary_text) if salary_text else None

        # Skills
        skill_elems = card.select(
            ".srp-skills span, .skill-list span, .tag"
        )
        skills = [s.get_text(strip=True) for s in skill_elems if s.get_text(strip=True)]

        # Date
        date_elem = card.select_one(
            ".sim-posted span, .date, [class*='date'], .posted-date"
        )
        date_posted = None
        if date_elem:
            date_posted = self._parse_date_str(date_elem.get_text(strip=True))

        # Description snippet
        desc_elem = card.select_one(
            ".list-job-dtl, .job-description, [class*='description']"
        )
        description = desc_elem.get_text(strip=True) if desc_elem else None

        return JobPost(
            id=f"tj-{job_id}",
            title=title,
            company_name=company,
            job_url=job_url,
            location=location,
            date_posted=date_posted,
            compensation=compensation,
            description=description,
            skills=skills if skills else None,
            experience_range=experience_range,
            emails=extract_emails_from_text(description or ""),
        )

    def _process_generic_card(self, card) -> Optional[JobPost]:
        """
        Processes a generic HTML card — last-resort extraction
        """
        # Find any link with a job-like URL
        link = card if card.name == "a" else card.select_one("a[href]")
        if not link or not link.get("href"):
            return None

        href = link["href"]
        if not any(kw in href for kw in ["/job/", "/jd/", "job-"]):
            return None

        title = link.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        job_url = href if href.startswith("http") else f"{self.base_url}{href}"
        id_match = re.search(r"(\d{6,})", href)
        job_id = id_match.group(1) if id_match else str(hash(title))

        return JobPost(
            id=f"tj-{job_id}",
            title=title,
            company_name=None,
            job_url=job_url,
            location=Location(country=Country.INDIA),
        )

    def _parse_salary(self, job: dict) -> Optional[Compensation]:
        """
        Parses salary from job dict
        """
        salary = job.get("salary") or job.get("salaryRange", "")
        if isinstance(salary, dict):
            min_val = salary.get("min") or salary.get("minValue")
            max_val = salary.get("max") or salary.get("maxValue")
            if min_val and max_val:
                return Compensation(
                    min_amount=int(float(min_val)),
                    max_amount=int(float(max_val)),
                    currency="INR",
                    interval=CompensationInterval.YEARLY,
                )
        elif isinstance(salary, str):
            return self._parse_salary_text(salary)
        return None

    def _parse_salary_text(self, text: str) -> Optional[Compensation]:
        """
        Parses salary text like '3-6 Lacs' into Compensation
        """
        if not text or text.lower() in ("not disclosed", "—", "n/a"):
            return None

        match = re.search(
            r"(\d+(?:\.\d+)?)\s*[-–]\s*(\d+(?:\.\d+)?)\s*(?:LPA|Lacs?|Lakh)",
            text,
            re.IGNORECASE,
        )
        if match:
            return Compensation(
                min_amount=int(float(match.group(1)) * 100000),
                max_amount=int(float(match.group(2)) * 100000),
                currency="INR",
                interval=CompensationInterval.YEARLY,
            )
        return None

    def _parse_date_str(self, date_str) -> Optional[date]:
        """
        Parses various date string formats
        """
        if not date_str:
            return None

        if isinstance(date_str, (int, float)):
            try:
                return datetime.fromtimestamp(date_str / 1000).date()
            except (ValueError, OSError):
                return None

        text = str(date_str).lower().strip()
        today = datetime.now()

        if "today" in text or "just now" in text:
            return today.date()

        day_match = re.search(r"(\d+)\s*day", text)
        if day_match:
            return (today - timedelta(days=int(day_match.group(1)))).date()

        week_match = re.search(r"(\d+)\s*week", text)
        if week_match:
            return (today - timedelta(weeks=int(week_match.group(1)))).date()

        # Try ISO date
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%b %d, %Y"):
            try:
                return datetime.strptime(text.split("t")[0] if "t" in text else text, fmt).date()
            except ValueError:
                continue

        return None
