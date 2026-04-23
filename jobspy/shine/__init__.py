# Shine.com Scraper for JobSpy
# ==============================
# Usage:
#   from jobspy import scrape_jobs
#   jobs = scrape_jobs(
#       site_name="shine",
#       search_term="software engineer",
#       location="Pune",
#       results_wanted=20,
#   )
#   print(jobs)
#
# Shine.com uses a search results page at:
#   https://www.shine.com/job-search/{keyword}-jobs-in-{city}
# Jobs are scraped via HTML parsing with BeautifulSoup.

from __future__ import annotations

import math
import random
import time
from datetime import datetime, date, timedelta
from typing import Optional

import regex as re
from bs4 import BeautifulSoup

from jobspy.exception import ShineException
from jobspy.model import (
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
from jobspy.util import (
    extract_emails_from_text,
    markdown_converter,
    create_session,
    create_logger,
)

log = create_logger("Shine")


class Shine(Scraper):
    base_url = "https://www.shine.com"
    delay = 2
    band_delay = 3
    jobs_per_page = 20

    def __init__(
        self,
        proxies: list[str] | str | None = None,
        ca_cert: str | None = None,
        user_agent: str | None = None,
    ):
        """
        Initializes ShineScraper for scraping shine.com job listings
        """
        super().__init__(Site.SHINE, proxies=proxies, ca_cert=ca_cert)
        self.session = create_session(
            proxies=self.proxies,
            ca_cert=ca_cert,
            is_tls=True,
            has_retry=False,
            delay=5,
        )
        self.session.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "user-agent": user_agent
            or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }
        self.scraper_input = None
        self.country = "India"
        log.info("Shine scraper initialized")

    def scrape(self, scraper_input: ScraperInput) -> JobResponse:
        """
        Scrapes Shine.com for jobs matching the scraper_input criteria
        """
        self.scraper_input = scraper_input
        job_list: list[JobPost] = []
        seen_ids: set[str] = set()
        page = 1
        request_count = 0

        continue_search = (
            lambda: len(job_list) < scraper_input.results_wanted and page <= 25
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
                    err = f"Shine response status code {response.status_code}"
                    log.error(err)
                    return JobResponse(jobs=job_list)

                soup = BeautifulSoup(response.text, "html.parser")

                # Try to find embedded JSON data first
                jobs_from_json = self._extract_from_script_data(soup)
                if jobs_from_json:
                    for job_post in jobs_from_json:
                        if job_post.id not in seen_ids:
                            seen_ids.add(job_post.id)
                            job_list.append(job_post)
                            if not continue_search():
                                break
                    if continue_search():
                        page += 1
                        time.sleep(random.uniform(self.delay, self.delay + self.band_delay))
                    continue

                # Fall back to HTML parsing
                job_cards = soup.select(
                    ".job_listing_row, .jobCard, .job-card, "
                    "[class*='jobCard'], [class*='job-listing']"
                )
                log.info(f"Found {len(job_cards)} job cards on page {page}")

                if not job_cards:
                    log.warning("No job cards found on page")
                    break

                for card in job_cards:
                    try:
                        job_post = self._process_html_card(card)
                        if job_post and job_post.id not in seen_ids:
                            seen_ids.add(job_post.id)
                            job_list.append(job_post)
                            if not continue_search():
                                break
                    except Exception as e:
                        log.warning(f"Error processing job card: {str(e)}")
                        continue

            except Exception as e:
                log.error(f"Shine request failed: {str(e)}")
                return JobResponse(jobs=job_list)

            if continue_search():
                time.sleep(random.uniform(self.delay, self.delay + self.band_delay))
                page += 1

        job_list = job_list[: scraper_input.results_wanted]
        log.info(f"Scraping completed. Total jobs collected: {len(job_list)}")
        return JobResponse(jobs=job_list)

    def _build_url(self, scraper_input: ScraperInput, page: int) -> str:
        """
        Builds the Shine.com search URL
        """
        keyword = (scraper_input.search_term or "").lower().replace(" ", "-")

        if scraper_input.location:
            city = scraper_input.location.lower().replace(" ", "-")
            path = f"/job-search/{keyword}-jobs-in-{city}"
        else:
            path = f"/job-search/{keyword}-jobs"

        if page > 1:
            path += f"-{page}"

        return f"{self.base_url}{path}"

    def _extract_from_script_data(self, soup: BeautifulSoup) -> list[JobPost]:
        """
        Tries to extract job data from embedded script tags (JSON-LD or __NEXT_DATA__)
        """
        import json

        jobs = []

        # Check for JSON-LD structured data
        for script in soup.select('script[type="application/ld+json"]'):
            try:
                data = json.loads(script.string)
                if isinstance(data, dict) and data.get("@type") == "ItemList":
                    for item in data.get("itemListElement", []):
                        job_data = item.get("item", item)
                        if job_data.get("@type") == "JobPosting":
                            job_post = self._parse_jsonld_job(job_data)
                            if job_post:
                                jobs.append(job_post)
                elif isinstance(data, dict) and data.get("@type") == "JobPosting":
                    job_post = self._parse_jsonld_job(data)
                    if job_post:
                        jobs.append(job_post)
            except (json.JSONDecodeError, AttributeError):
                continue

        # Check for __NEXT_DATA__ or similar embedded JSON
        for script in soup.select("script#__NEXT_DATA__"):
            try:
                data = json.loads(script.string)
                props = data.get("props", {}).get("pageProps", {})
                job_list = props.get("jobs", props.get("jobList", props.get("results", [])))
                for job in job_list:
                    job_post = self._parse_api_job(job)
                    if job_post:
                        jobs.append(job_post)
            except (json.JSONDecodeError, AttributeError):
                continue

        return jobs

    def _parse_jsonld_job(self, data: dict) -> Optional[JobPost]:
        """
        Parses a JobPosting JSON-LD object into a JobPost
        """
        title = data.get("title", "")
        if not title:
            return None

        company = ""
        if data.get("hiringOrganization"):
            org = data["hiringOrganization"]
            company = org.get("name", "") if isinstance(org, dict) else str(org)

        # Location
        loc_data = data.get("jobLocation", {})
        if isinstance(loc_data, dict):
            address = loc_data.get("address", {})
            city = address.get("addressLocality", "")
            state = address.get("addressRegion", "")
        elif isinstance(loc_data, list) and loc_data:
            address = loc_data[0].get("address", {})
            city = address.get("addressLocality", "")
            state = address.get("addressRegion", "")
        else:
            city, state = "", ""

        location = Location(
            city=city or None, state=state or None, country=Country.INDIA
        )

        # Date
        date_posted = None
        date_str = data.get("datePosted")
        if date_str:
            try:
                date_posted = datetime.strptime(date_str[:10], "%Y-%m-%d").date()
            except ValueError:
                pass

        # URL
        job_url = data.get("url", "")
        if job_url and not job_url.startswith("http"):
            job_url = f"{self.base_url}{job_url}"

        # Salary
        compensation = None
        salary_data = data.get("baseSalary", {})
        if salary_data and isinstance(salary_data, dict):
            value = salary_data.get("value", {})
            if isinstance(value, dict):
                min_val = value.get("minValue")
                max_val = value.get("maxValue")
                if min_val and max_val:
                    compensation = Compensation(
                        min_amount=int(float(min_val)),
                        max_amount=int(float(max_val)),
                        currency=salary_data.get("currency", "INR"),
                        interval=CompensationInterval.YEARLY,
                    )

        # Description
        description = data.get("description", "")
        if (
            description
            and self.scraper_input
            and self.scraper_input.description_format == DescriptionFormat.MARKDOWN
        ):
            description = markdown_converter(description)

        # Skills
        skills_raw = data.get("skills")
        skills = None
        if isinstance(skills_raw, str):
            skills = [s.strip() for s in skills_raw.split(",") if s.strip()]
        elif isinstance(skills_raw, list):
            skills = skills_raw

        job_id = data.get("identifier", {}).get("value", str(hash(title + company)))

        return JobPost(
            id=f"sh-{job_id}",
            title=title,
            company_name=company or None,
            job_url=job_url or f"{self.base_url}/job-search/",
            location=location,
            date_posted=date_posted,
            compensation=compensation,
            description=description,
            skills=skills,
            emails=extract_emails_from_text(description or ""),
        )

    def _parse_api_job(self, job: dict) -> Optional[JobPost]:
        """
        Parses a job from embedded API data (e.g., __NEXT_DATA__)
        """
        title = job.get("title") or job.get("jobTitle", "")
        if not title:
            return None

        company = job.get("companyName") or job.get("company", "")
        job_id = str(job.get("id") or job.get("jobId") or hash(title + company))

        # Location
        loc = job.get("location") or job.get("city", "")
        if isinstance(loc, list):
            loc = loc[0] if loc else ""
        parts = loc.split(",") if isinstance(loc, str) else []
        location = Location(
            city=parts[0].strip() if parts else None,
            state=parts[1].strip() if len(parts) > 1 else None,
            country=Country.INDIA,
        )

        # URL
        slug = job.get("slug") or job.get("seoUrl", "")
        job_url = (
            f"{self.base_url}/job/{slug}"
            if slug
            else f"{self.base_url}/job/{job_id}"
        )

        # Salary
        compensation = None
        salary = job.get("salary") or job.get("salaryRange", "")
        if isinstance(salary, dict):
            min_val = salary.get("min") or salary.get("minValue")
            max_val = salary.get("max") or salary.get("maxValue")
            if min_val and max_val:
                compensation = Compensation(
                    min_amount=int(float(min_val)),
                    max_amount=int(float(max_val)),
                    currency="INR",
                    interval=CompensationInterval.YEARLY,
                )

        # Experience
        exp = job.get("experience") or job.get("experienceRange", "")
        experience_range = str(exp) if exp else None

        # Skills
        skills_raw = job.get("skills") or job.get("keySkills", [])
        if isinstance(skills_raw, str):
            skills = [s.strip() for s in skills_raw.split(",") if s.strip()]
        elif isinstance(skills_raw, list):
            skills = skills_raw
        else:
            skills = None

        # Description
        description = job.get("description") or job.get("jobDescription", "")

        return JobPost(
            id=f"sh-{job_id}",
            title=title,
            company_name=company or None,
            job_url=job_url,
            location=location,
            compensation=compensation,
            description=description,
            skills=skills if skills else None,
            experience_range=experience_range,
            emails=extract_emails_from_text(description or ""),
        )

    def _process_html_card(self, card) -> Optional[JobPost]:
        """
        Processes a single HTML job card into a JobPost
        """
        # Title
        title_elem = card.select_one(
            "a.job_title, h3 a, .jobTitle a, a[class*='title'], "
            ".job-title a, a.jobCard__title"
        )
        if not title_elem:
            title_elem = card.select_one("a")
        title = title_elem.get_text(strip=True) if title_elem else None
        if not title:
            return None

        # URL
        href = title_elem.get("href", "") if title_elem else ""
        job_url = href if href.startswith("http") else f"{self.base_url}{href}"

        # Job ID from URL
        id_match = re.search(r"[/-](\d{6,})", href)
        job_id = id_match.group(1) if id_match else str(hash(title))

        # Company
        company_elem = card.select_one(
            ".company_name, .companyName, [class*='company'], .job-company"
        )
        company = company_elem.get_text(strip=True) if company_elem else None

        # Location
        loc_elem = card.select_one(
            ".loc, .location, [class*='location'], .job-location"
        )
        loc_text = loc_elem.get_text(strip=True) if loc_elem else ""
        parts = [p.strip() for p in loc_text.split(",")] if loc_text else []
        location = Location(
            city=parts[0] if parts else None,
            state=parts[1] if len(parts) > 1 else None,
            country=Country.INDIA,
        )

        # Salary
        salary_elem = card.select_one(
            ".salary, .sal, [class*='salary'], .job-salary"
        )
        salary_text = salary_elem.get_text(strip=True) if salary_elem else ""
        compensation = self._parse_salary_text(salary_text) if salary_text else None

        # Experience
        exp_elem = card.select_one(
            ".exp, .experience, [class*='experience'], .job-exp"
        )
        experience_range = exp_elem.get_text(strip=True) if exp_elem else None

        # Skills
        skill_elems = card.select(".skill, .tag, [class*='skill'] span")
        skills = [s.get_text(strip=True) for s in skill_elems] if skill_elems else None

        # Date
        date_elem = card.select_one(
            ".date, .postedDate, [class*='date'], .job-date"
        )
        date_posted = None
        if date_elem:
            date_posted = self._parse_date_text(date_elem.get_text(strip=True))

        return JobPost(
            id=f"sh-{job_id}",
            title=title,
            company_name=company,
            job_url=job_url,
            location=location,
            date_posted=date_posted,
            compensation=compensation,
            skills=skills if skills else None,
            experience_range=experience_range,
        )

    def _parse_salary_text(self, text: str) -> Optional[Compensation]:
        """
        Parses salary text into Compensation
        """
        if not text or text.lower() in ("not disclosed", "—", "n/a"):
            return None

        # "3-6 LPA" or "₹ 3 - 5 Lacs P.A."
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

    def _parse_date_text(self, text: str) -> Optional[date]:
        """
        Parses relative date text
        """
        if not text:
            return None
        text = text.lower()
        today = datetime.now()

        if "today" in text or "just now" in text:
            return today.date()

        day_match = re.search(r"(\d+)\s*day", text)
        if day_match:
            return (today - timedelta(days=int(day_match.group(1)))).date()

        week_match = re.search(r"(\d+)\s*week", text)
        if week_match:
            return (today - timedelta(weeks=int(week_match.group(1)))).date()

        return None
