# Internshala Scraper for JobSpy
# ================================
# Usage:
#   from jobspy import scrape_jobs
#   jobs = scrape_jobs(
#       site_name="internshala",
#       search_term="software engineer",
#       location="Pune",
#       results_wanted=20,
#       hours_old=72,
#   )
#   print(jobs)
#
# Internshala serves HTML pages (not JSON APIs). This scraper parses the HTML
# job listing pages at:
#   https://internshala.com/jobs/{keyword}-jobs-in-{city}/
#   https://internshala.com/jobs/{keyword}-jobs/  (if no city specified)
# Pagination uses: /page-{n}/

from __future__ import annotations

import math
import random
import time
from datetime import datetime, date, timedelta
from typing import Optional

import regex as re
from bs4 import BeautifulSoup

from jobspy.exception import InternshalaException
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

log = create_logger("Internshala")


class Internshala(Scraper):
    base_url = "https://internshala.com"
    delay = 2
    band_delay = 3
    jobs_per_page = 20  # Internshala shows ~20 jobs per page

    def __init__(
        self,
        proxies: list[str] | str | None = None,
        ca_cert: str | None = None,
        user_agent: str | None = None,
    ):
        """
        Initializes IntershalaScraper for scraping internshala.com job listings
        """
        super().__init__(Site.INTERNSHALA, proxies=proxies, ca_cert=ca_cert)
        self.session = create_session(
            proxies=self.proxies,
            ca_cert=ca_cert,
            is_tls=False,
            has_retry=True,
            delay=5,
            clear_cookies=False,
        )
        self.session.headers.update({
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "user-agent": user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        })
        self.scraper_input = None
        self.country = "India"
        log.info("Internshala scraper initialized")

    def scrape(self, scraper_input: ScraperInput) -> JobResponse:
        """
        Scrapes Internshala for jobs matching the scraper_input criteria
        :param scraper_input: ScraperInput with search params
        :return: JobResponse containing matched jobs
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
                response = self.session.get(url, timeout=15)
                if response.status_code not in range(200, 400):
                    err = f"Internshala response status code {response.status_code}"
                    log.error(err)
                    return JobResponse(jobs=job_list)

                soup = BeautifulSoup(response.text, "html.parser")
                job_cards = self._extract_job_cards(soup)
                log.info(f"Found {len(job_cards)} job cards on page {page}")

                if not job_cards:
                    log.warning("No job cards found on page")
                    break

            except Exception as e:
                log.error(f"Internshala request failed: {str(e)}")
                return JobResponse(jobs=job_list)

            for card in job_cards:
                try:
                    job_id = self._get_job_id(card)
                    if not job_id or job_id in seen_ids:
                        continue
                    seen_ids.add(job_id)

                    job_post = self._process_job_card(card, job_id)
                    if job_post:
                        job_list.append(job_post)
                        log.info(f"Added job: {job_post.title} (ID: {job_id})")
                    if not continue_search():
                        break
                except Exception as e:
                    log.warning(f"Error processing job card: {str(e)}")
                    continue

            if continue_search():
                time.sleep(random.uniform(self.delay, self.delay + self.band_delay))
                page += 1

        job_list = job_list[: scraper_input.results_wanted]
        log.info(f"Scraping completed. Total jobs collected: {len(job_list)}")
        return JobResponse(jobs=job_list)

    def _build_url(self, scraper_input: ScraperInput, page: int) -> str:
        """
        Builds the Internshala search URL from scraper input
        """
        keyword_slug = (
            scraper_input.search_term.lower().replace(" ", "-")
            if scraper_input.search_term
            else ""
        )

        # Determine if we're looking at jobs or internships
        # Default to jobs section
        section = "jobs"

        if scraper_input.location:
            city_slug = scraper_input.location.lower().replace(" ", "-")
            path = f"/{section}/{keyword_slug}-jobs-in-{city_slug}/"
        else:
            path = f"/{section}/{keyword_slug}-jobs/"

        # Add work-from-home filter if remote
        if scraper_input.is_remote:
            path = path.rstrip("/") + "/work-from-home/"

        # Add pagination
        if page > 1:
            path = path.rstrip("/") + f"/page-{page}/"

        return f"{self.base_url}{path}"

    def _extract_job_cards(self, soup: BeautifulSoup) -> list:
        """
        Extracts job card elements from the Internshala HTML page.
        Internshala uses various container classes for job cards.
        """
        # Try different selectors Internshala uses
        cards = soup.select(".individual_internship")
        if not cards:
            cards = soup.select(".internship_meta")
        if not cards:
            cards = soup.select("[data-internship-id]")
        if not cards:
            # Try broader selector for job listings
            cards = soup.select(".container-fluid.individual_internship")
        if not cards:
            # Last resort: look for heading links in the job listing area
            cards = soup.select(".internship_list_container .individual_internship")

        return cards

    def _get_job_id(self, card) -> str | None:
        """
        Extracts the job/internship ID from the card element
        """
        # Try data attribute first
        job_id = card.get("data-internship-id") or card.get("data-job-id")
        if job_id:
            return str(job_id)

        # Try extracting from the link
        link = card.select_one("a.job-title-href, a.view_detail_button, h3 a, .heading_4_5 a")
        if link and link.get("href"):
            href = link["href"]
            # Extract numeric ID from URL like /job/detail/...-1768609814
            match = re.search(r"(\d{8,})", href)
            if match:
                return match.group(1)

        # Fallback: use the card's id attribute
        if card.get("id"):
            return card["id"]

        return None

    def _process_job_card(self, card, job_id: str) -> Optional[JobPost]:
        """
        Processes a single job card HTML element into a JobPost
        """
        # Title
        title_elem = card.select_one(
            "h3 a, .heading_4_5 a, a.job-title-href, .job-internship-name a, "
            ".profile a, .heading_4_5"
        )
        title = title_elem.get_text(strip=True) if title_elem else None
        if not title:
            return None

        # Job URL
        job_url = None
        if title_elem and title_elem.name == "a" and title_elem.get("href"):
            href = title_elem["href"]
            job_url = href if href.startswith("http") else f"{self.base_url}{href}"
        if not job_url:
            link = card.select_one("a[href*='/job/detail/'], a[href*='/internship/detail/']")
            if link:
                href = link["href"]
                job_url = href if href.startswith("http") else f"{self.base_url}{href}"
            else:
                job_url = f"{self.base_url}/job/detail/{job_id}"

        # Company name
        company_elem = card.select_one(
            ".company_name, .link_display_like_text, .company-name, "
            "p.company_name a, .heading_6"
        )
        company_name = company_elem.get_text(strip=True) if company_elem else None

        # Location
        location = self._parse_location(card)

        # Salary / Stipend
        salary_text = None
        stipend = None
        salary_elem = card.select_one(
            ".salary, .stipend, span.desktop-text:-soup-contains('salary'), "
            ".ic-16-money + span, .item_body:has(.ic-16-money)"
        )
        if not salary_elem:
            # Try finding salary within item_body spans
            for span in card.select(".item_body"):
                text = span.get_text(strip=True)
                if "₹" in text or "lakh" in text.lower() or "month" in text.lower():
                    salary_elem = span
                    break

        if salary_elem:
            salary_text = salary_elem.get_text(strip=True)
            stipend = salary_text

        compensation = self._parse_compensation(salary_text) if salary_text else None

        # Date posted
        date_posted = self._parse_date(card)

        # Skills
        skills = self._parse_skills(card)

        # Remote check
        is_remote = self._check_remote(card, title, location)

        # Determine if internship vs job
        is_internship = self._check_is_internship(card, job_url or "")

        # Job type
        job_type = self._infer_job_type(card, title, is_internship)

        # Apply by date
        apply_by = self._parse_apply_by(card)

        # Experience range
        experience_range = self._parse_experience(card)

        # Description (brief, from card — full description needs detail page fetch)
        description = None
        desc_elem = card.select_one(
            ".internship_other_details_container, .detail_view, .job_description"
        )
        if desc_elem:
            description = desc_elem.get_text(strip=True)

        job_post = JobPost(
            id=f"is-{job_id}",
            title=title,
            company_name=company_name,
            job_url=job_url,
            location=location,
            date_posted=date_posted,
            compensation=compensation,
            job_type=job_type,
            is_remote=is_remote,
            description=description,
            emails=extract_emails_from_text(description or ""),
            skills=skills,
            experience_range=experience_range,
            is_internship=is_internship,
            stipend=stipend,
            apply_by=apply_by,
        )
        log.debug(f"Processed job: {title} at {company_name}")
        return job_post

    def _parse_location(self, card) -> Location:
        """
        Extracts location from the job card
        """
        location_elem = card.select_one(
            ".location_link, .individual_location_name, #location_names a, "
            "#location_names span, .locations a, .ic-16-map-pin + span"
        )
        if not location_elem:
            # Broader search
            for elem in card.select("a, span"):
                parent_class = " ".join(elem.parent.get("class", []))
                if "location" in parent_class.lower():
                    location_elem = elem
                    break

        if location_elem:
            loc_text = location_elem.get_text(strip=True)
            parts = [p.strip() for p in loc_text.split(",")]
            city = parts[0] if parts else None
            state = parts[1] if len(parts) > 1 else None
            return Location(city=city, state=state, country=Country.INDIA)

        return Location(country=Country.INDIA)

    def _parse_compensation(self, salary_text: str) -> Optional[Compensation]:
        """
        Parses salary/stipend text into a Compensation object.
        Handles formats like:
        - '₹ 3,00,000 - 6,00,000 /year'
        - '₹ 15,000 /month'
        - '3-6 LPA'
        - '₹ 3 - 5 Lacs P.A.'
        """
        if not salary_text or salary_text.lower() in ("unpaid", "not disclosed", "—"):
            return None

        # Try LPA / Lacs format first (e.g., "3-6 LPA", "₹ 3 - 5 Lacs P.A.")
        lpa_match = re.search(
            r"(\d+(?:\.\d+)?)\s*[-–—]\s*(\d+(?:\.\d+)?)\s*(?:LPA|Lacs?|Lakh)",
            salary_text,
            re.IGNORECASE,
        )
        if lpa_match:
            min_val = float(lpa_match.group(1)) * 100000
            max_val = float(lpa_match.group(2)) * 100000
            return Compensation(
                min_amount=int(min_val),
                max_amount=int(max_val),
                currency="INR",
                interval=CompensationInterval.YEARLY,
            )

        # Try absolute INR range (e.g., "₹ 3,00,000 - 6,00,000")
        abs_match = re.search(
            r"₹?\s*([\d,]+)\s*[-–—]\s*₹?\s*([\d,]+)", salary_text
        )
        if abs_match:
            min_val = int(abs_match.group(1).replace(",", ""))
            max_val = int(abs_match.group(2).replace(",", ""))

            # Determine interval from text
            if "/month" in salary_text.lower() or "month" in salary_text.lower():
                interval = CompensationInterval.MONTHLY
            elif "/year" in salary_text.lower() or "annum" in salary_text.lower():
                interval = CompensationInterval.YEARLY
            else:
                # If values > 100000, assume yearly; else monthly
                interval = (
                    CompensationInterval.YEARLY
                    if max_val > 100000
                    else CompensationInterval.MONTHLY
                )

            return Compensation(
                min_amount=min_val,
                max_amount=max_val,
                currency="INR",
                interval=interval,
            )

        # Try single value (e.g., "₹ 15,000 /month")
        single_match = re.search(r"₹?\s*([\d,]+)", salary_text)
        if single_match:
            amount = int(single_match.group(1).replace(",", ""))
            if "/month" in salary_text.lower() or "month" in salary_text.lower():
                interval = CompensationInterval.MONTHLY
            else:
                interval = CompensationInterval.YEARLY

            return Compensation(
                min_amount=amount,
                max_amount=amount,
                currency="INR",
                interval=interval,
            )

        return None

    def _parse_date(self, card) -> Optional[date]:
        """
        Parses the posting date from a job card
        """
        date_elem = card.select_one(
            ".date, .posted_by_container span, .status-success, .status-info, "
            ".ic-16-calendar + span"
        )
        if not date_elem:
            # Look for text containing time indicators
            for elem in card.select("span, div"):
                text = elem.get_text(strip=True).lower()
                if any(kw in text for kw in ["ago", "day", "today", "just now", "week", "hour"]):
                    date_elem = elem
                    break

        if not date_elem:
            return None

        text = date_elem.get_text(strip=True).lower()
        today = datetime.now()

        if "just now" in text or "today" in text:
            return today.date()

        day_match = re.search(r"(\d+)\s*day", text)
        if day_match:
            return (today - timedelta(days=int(day_match.group(1)))).date()

        week_match = re.search(r"(\d+)\s*week", text)
        if week_match:
            return (today - timedelta(weeks=int(week_match.group(1)))).date()

        hour_match = re.search(r"(\d+)\s*hour", text)
        if hour_match:
            return today.date()

        month_match = re.search(r"(\d+)\s*month", text)
        if month_match:
            return (today - timedelta(days=int(month_match.group(1)) * 30)).date()

        return None

    def _parse_skills(self, card) -> list[str] | None:
        """
        Extracts skills/tags from the job card
        """
        skill_elems = card.select(
            ".round_tabs, .skill_tag, .individual_skill, "
            ".tags .tag, .individual_internship_tag"
        )
        if skill_elems:
            skills = [s.get_text(strip=True) for s in skill_elems if s.get_text(strip=True)]
            return skills if skills else None
        return None

    def _check_remote(self, card, title: str, location: Location) -> bool:
        """
        Checks if the job is remote/WFH
        """
        remote_keywords = ["remote", "work from home", "wfh"]

        # Check card text
        card_text = card.get_text(strip=True).lower()
        if any(kw in card_text for kw in remote_keywords):
            return True

        # Check title
        if title and any(kw in title.lower() for kw in remote_keywords):
            return True

        # Check location
        loc_str = location.display_location().lower()
        if any(kw in loc_str for kw in remote_keywords):
            return True

        return False

    def _check_is_internship(self, card, job_url: str) -> bool:
        """
        Determines if this is an internship vs a full-time job
        """
        if "/internship/" in job_url:
            return True

        card_text = card.get_text(strip=True).lower()
        if "internship" in card_text:
            return True

        return False

    def _infer_job_type(
        self, card, title: str, is_internship: bool
    ) -> list[JobType] | None:
        """
        Infers job type from card content
        """
        if is_internship:
            return [JobType.INTERNSHIP]

        card_text = card.get_text(strip=True).lower()
        types = []

        if "full time" in card_text or "full-time" in card_text:
            types.append(JobType.FULL_TIME)
        if "part time" in card_text or "part-time" in card_text:
            types.append(JobType.PART_TIME)
        if "contract" in card_text:
            types.append(JobType.CONTRACT)

        if title:
            title_lower = title.lower()
            if "intern" in title_lower:
                types.append(JobType.INTERNSHIP)

        return types if types else None

    def _parse_apply_by(self, card) -> str | None:
        """
        Extracts the application deadline
        """
        for elem in card.select("span, div, p"):
            text = elem.get_text(strip=True)
            if "apply by" in text.lower():
                # Extract the date part after "Apply by"
                match = re.search(r"apply\s*by\s*[:\s]*(.+)", text, re.IGNORECASE)
                if match:
                    return match.group(1).strip()
        return None

    def _parse_experience(self, card) -> str | None:
        """
        Extracts experience requirement from the card
        """
        for elem in card.select("span, div, p"):
            text = elem.get_text(strip=True).lower()
            if "experience" in text or "year" in text:
                exp_match = re.search(r"(\d+\s*[-–]\s*\d+\s*(?:year|yr)s?)", text, re.IGNORECASE)
                if exp_match:
                    return exp_match.group(1)
                fresher_match = re.search(r"(fresher|0\s*[-–]\s*\d+\s*(?:year|yr))", text, re.IGNORECASE)
                if fresher_match:
                    return fresher_match.group(1)
        return None
