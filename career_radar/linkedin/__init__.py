from __future__ import annotations

import math
import random
import time
from datetime import datetime
from typing import Optional, Any
from urllib.parse import urlparse, urlunparse, unquote

import regex as re
from bs4 import BeautifulSoup
from bs4.element import Tag

from career_radar.linkedin.constant import headers
from career_radar.linkedin.util import (
    is_job_remote,
    job_type_code,
    parse_job_type,
    parse_job_level,
    parse_company_industry
)
from career_radar.model import (
    JobPost,
    Location,
    JobResponse,
    Country,
    Compensation,
    DescriptionFormat,
    Scraper,
    ScraperInput,
    Site,
)
from career_radar.util import (
    extract_emails_from_text,
    currency_parser,
    markdown_converter,
    plain_converter,
    create_session,
    remove_attributes,
    create_logger,
)

log = create_logger("LinkedIn")


class LinkedIn(Scraper):
    base_url = "https://www.linkedin.com"
    jobs_per_page = 10
    min_delay_ms = 800  # P3: LinkedIn minimum delay enforcement
    max_retries = 4
    transient_statuses = {408, 425, 429, 500, 502, 503, 504}

    def _get_delay(self) -> float:
        """
        P3: Compute delay between requests in seconds.
        Enforces minimum 800ms for LinkedIn regardless of input.
        Supports jitter if tuple (min_ms, max_ms) is provided.
        """
        delay_config = self.scraper_input.delay_between_requests_ms if self.scraper_input else 1000

        if isinstance(delay_config, tuple):
            min_ms, max_ms = delay_config
            delay_ms = random.uniform(min_ms, max_ms)
        else:
            delay_ms = delay_config

        # Enforce LinkedIn minimum of 800ms
        delay_ms = max(delay_ms, self.min_delay_ms)
        return delay_ms / 1000.0  # Convert to seconds

    def __init__(
        self, proxies: list[str] | str | None = None, ca_cert: str | None = None, user_agent: str | None = None,
        linkedin_session_cookie: str | None = None
    ):
        """
        Initializes LinkedInScraper with the LinkedIn job search url
        :param linkedin_session_cookie: Optional li_at session cookie for authenticated requests.
            This is the user's own session cookie - not stored by the library.
        """
        super().__init__(Site.LINKEDIN, proxies=proxies, ca_cert=ca_cert, user_agent=user_agent)
        self.session = create_session(
            proxies=self.proxies,
            ca_cert=ca_cert,
            is_tls=False,
            has_retry=True,
            delay=5,
            clear_cookies=True,
            user_agent=user_agent,
        )
        self.session.headers.update(headers)
        # P4: Add additional headers to avoid blocking
        self.session.headers["Accept-Language"] = "en-US,en;q=0.9"
        self.session.headers["Referer"] = "https://www.linkedin.com/"
        # P4: Inject session cookie if provided
        if linkedin_session_cookie:
            self.session.cookies.set("li_at", linkedin_session_cookie)
        self.scraper_input = None
        self.country = "worldwide"
        self.job_url_direct_regex = re.compile(r'(?<=\?url=)[^"]+')

    def scrape(self, scraper_input: ScraperInput) -> JobResponse:
        """
        Scrapes LinkedIn for jobs with scraper_input criteria
        :param scraper_input:
        :return: job_response
        """
        self.scraper_input = scraper_input
        job_list: list[JobPost] = []
        seen_ids = set()
        start = scraper_input.offset // 10 * 10 if scraper_input.offset else 0
        request_count = 0
        empty_page_streak = 0
        seconds_old = (
            scraper_input.hours_old * 3600 if scraper_input.hours_old else None
        )
        continue_search = (
            lambda: len(job_list) < scraper_input.results_wanted and start < 1000
        )
        while continue_search():
            request_count += 1
            log.info(
                f"search page: {request_count} / {math.ceil(scraper_input.results_wanted / 10)}"
            )
            params = {
                "keywords": scraper_input.search_term,
                "location": scraper_input.location,
                "distance": scraper_input.distance,
                "f_WT": 2 if scraper_input.is_remote else None,
                "f_JT": (
                    job_type_code(scraper_input.job_type)
                    if scraper_input.job_type
                    else None
                ),
                "pageNum": 0,
                "start": start,
                "f_AL": "true" if scraper_input.easy_apply else None,
                "f_C": (
                    ",".join(map(str, scraper_input.linkedin_company_ids))
                    if scraper_input.linkedin_company_ids
                    else None
                ),
            }
            if seconds_old is not None:
                params["f_TPR"] = f"r{seconds_old}"

            params = {k: v for k, v in params.items() if v is not None}
            response = self._request_with_backoff(
                url=f"{self.base_url}/jobs-guest/jobs/api/seeMoreJobPostings/search?",
                params=params,
                timeout=10,
                context="search page",
            )
            if response is None:
                break

            text_lower = response.text.lower()
            if "captcha" in text_lower or "security verification" in text_lower:
                log.warning("LinkedIn challenge page detected. Returning partial results.")
                break

            soup = BeautifulSoup(response.text, "html.parser")
            job_cards = soup.find_all("div", class_="base-search-card")
            if len(job_cards) == 0:
                empty_page_streak += 1
                if empty_page_streak >= 2:
                    break
                start += self.jobs_per_page
                continue
            empty_page_streak = 0

            for job_card in job_cards:
                href_tag = job_card.find("a", class_="base-card__full-link")
                if href_tag and "href" in href_tag.attrs:
                    href = href_tag.attrs["href"].split("?")[0]
                    job_id = self._extract_job_id(href)
                    if not job_id:
                        continue

                    if job_id in seen_ids:
                        continue
                    seen_ids.add(job_id)

                    try:
                        fetch_desc = scraper_input.linkedin_fetch_description
                        job_post = self._process_job(job_card, job_id, fetch_desc)
                        if job_post:
                            job_list.append(job_post)
                        if not continue_search():
                            break
                    except Exception as e:
                        log.warning("Failed to process LinkedIn card %s: %s", job_id, e)
                        continue

            if continue_search():
                # P3: Use configurable delay instead of hardcoded
                sleep_time = self._get_delay()
                time.sleep(sleep_time)
                start += max(len(job_cards), self.jobs_per_page)

        job_list = job_list[: scraper_input.results_wanted]
        return JobResponse(jobs=job_list)

    def _process_job(
        self, job_card: Tag, job_id: str, full_descr: bool
    ) -> Optional[JobPost]:
        salary_tag = job_card.find("span", class_="job-search-card__salary-info")

        compensation = description = None
        if salary_tag:
            salary_text = salary_tag.get_text(separator=" ").strip()
            parts = [segment.strip() for segment in re.split(r"[-–—]", salary_text) if segment.strip()]
            try:
                if parts:
                    parsed = [currency_parser(value) for value in parts]
                    salary_min = parsed[0]
                    salary_max = parsed[-1]
                    currency = salary_text[0] if salary_text and salary_text[0] != "$" else "USD"
                    compensation = Compensation(
                        min_amount=int(salary_min),
                        max_amount=int(salary_max),
                        currency=currency,
                    )
            except Exception:
                compensation = None

        title_tag = job_card.find("span", class_="sr-only")
        title = title_tag.get_text(strip=True) if title_tag else ""
        if not title:
            title = "N/A"

        company_tag = job_card.find("h4", class_="base-search-card__subtitle")
        company_a_tag = company_tag.find("a") if company_tag else None
        company_url = ""
        if company_a_tag and company_a_tag.has_attr("href"):
            try:
                company_url = urlunparse(urlparse(company_a_tag.get("href"))._replace(query=""))
            except Exception:
                company_url = company_a_tag.get("href") or ""
        company = company_a_tag.get_text(strip=True) if company_a_tag else "N/A"

        metadata_card = job_card.find("div", class_="base-search-card__metadata")
        location = self._get_location(metadata_card)

        datetime_tag = (
            metadata_card.find("time", class_="job-search-card__listdate")
            if metadata_card
            else None
        )
        if not datetime_tag and metadata_card:
            datetime_tag = metadata_card.find(
                "time", class_="job-search-card__listdate--new"
            )
        date_posted = None
        if datetime_tag and "datetime" in datetime_tag.attrs:
            datetime_str = datetime_tag["datetime"]
            try:
                date_posted = datetime.strptime(datetime_str, "%Y-%m-%d").date()
            except Exception:
                date_posted = None
        job_details = {}
        if full_descr:
            job_details = self._get_job_details(job_id)
            description = job_details.get("description")
        if description is None:
            short_desc_tag = job_card.find("p", class_="job-search-card__snippet")
            description = short_desc_tag.get_text(" ", strip=True) if short_desc_tag else None
        is_remote = is_job_remote(title, description, location)
        job_level = job_details.get("job_level")
        if isinstance(job_level, str):
            job_level = job_level.lower()

        return JobPost(
            id=f"li-{job_id}",
            title=title,
            company_name=company,
            company_url=company_url,
            location=location,
            is_remote=is_remote,
            date_posted=date_posted,
            job_url=f"{self.base_url}/jobs/view/{job_id}",
            compensation=compensation,
            job_type=job_details.get("job_type"),
            job_level=job_level,
            company_industry=job_details.get("company_industry"),
            description=description,
            job_url_direct=job_details.get("job_url_direct"),
            emails=extract_emails_from_text(description) if description else None,
            company_logo=job_details.get("company_logo"),
            job_function=job_details.get("job_function"),
        )

    def _get_job_details(self, job_id: str) -> dict:
        """
        Retrieves job description and other job details by going to the job page url
        :param job_page_url:
        :return: dict
        """
        response = self._request_with_backoff(
            url=f"{self.base_url}/jobs/view/{job_id}",
            params=None,
            timeout=8,
            context=f"job detail {job_id}",
        )
        if response is None:
            return {}
        if "linkedin.com/signup" in response.url:
            return {}

        soup = BeautifulSoup(response.text, "html.parser")
        div_content = soup.find(
            "div", class_=lambda x: x and "show-more-less-html__markup" in x
        )
        description = None
        if div_content is not None:
            div_content = remove_attributes(div_content)
            description = div_content.prettify(formatter="html")
            if self.scraper_input.description_format == DescriptionFormat.MARKDOWN:
                description = markdown_converter(description)
            elif self.scraper_input.description_format == DescriptionFormat.PLAIN:
                description = plain_converter(description)
        h3_tag = soup.find(
            "h3", string=lambda text: text and "Job function" in text.strip()
        )

        job_function = None
        if h3_tag:
            job_function_span = h3_tag.find_next(
                "span", class_="description__job-criteria-text"
            )
            if job_function_span:
                job_function = job_function_span.text.strip()

        company_logo = (
            logo_image.get("data-delayed-url")
            if (logo_image := soup.find("img", {"class": "artdeco-entity-image"}))
            else None
        )
        return {
            "description": description,
            "job_level": parse_job_level(soup),
            "company_industry": parse_company_industry(soup),
            "job_type": parse_job_type(soup),
            "job_url_direct": self._parse_job_url_direct(soup),
            "company_logo": company_logo,
            "job_function": job_function,
        }

    def _get_location(self, metadata_card: Optional[Tag]) -> Location:
        """
        Extracts the location data from the job metadata card.
        :param metadata_card
        :return: location
        """
        location = Location(country=self._safe_country(self.country))
        if metadata_card is not None:
            location_tag = metadata_card.find(
                "span", class_="job-search-card__location"
            )
            location_string = location_tag.text.strip() if location_tag else "N/A"
            parts = location_string.split(", ")
            if len(parts) == 2:
                city, state = parts
                location = Location(
                    city=city,
                    state=state,
                    country=self._safe_country(self.country),
                )
            elif len(parts) == 3:
                city, state, country = parts
                country = self._safe_country(country)
                location = Location(city=city, state=state, country=country)
            elif len(parts) == 1 and parts[0] != "N/A":
                location = Location(city=parts[0], country=self._safe_country(self.country))
        return location

    def _safe_country(self, value: str) -> Country | str:
        """Safely maps country text to Country enum, preserving unknowns as raw strings."""
        try:
            return Country.from_string(value)
        except Exception:
            return value

    def _extract_job_id(self, href: str) -> str | None:
        """Extracts LinkedIn numeric job id from href."""
        if not href:
            return None
        match = re.search(r"/jobs/view/(?:[\w-]+-)?(\d+)", href)
        if match:
            return match.group(1)
        fallback = href.split("-")[-1]
        return fallback if fallback.isdigit() else None

    def _request_with_backoff(
        self,
        *,
        url: str,
        params: dict[str, Any] | None,
        timeout: int,
        context: str,
    ):
        """Executes resilient GET requests with retry/backoff on transient errors."""
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=timeout)
            except Exception as exc:
                if attempt == self.max_retries:
                    log.error("LinkedIn %s failed: %s", context, exc)
                    return None
                self._retry_sleep(attempt, f"{context} exception")
                continue

            if response.status_code in self.transient_statuses:
                if attempt == self.max_retries:
                    log.warning("LinkedIn %s transient status persisted: %s", context, response.status_code)
                    return None
                self._retry_sleep(attempt, f"{context} status={response.status_code}")
                continue

            if response.status_code not in range(200, 400):
                log.warning("LinkedIn %s non-success status: %s", context, response.status_code)
                return None
            return response
        return None

    def _retry_sleep(self, attempt: int, reason: str) -> None:
        """Backoff helper for LinkedIn retries with jitter."""
        base_delay = self._get_delay()
        wait = (2 ** (attempt - 1)) * base_delay + random.uniform(0.2, 1.0)
        log.warning("LinkedIn retry in %.1fs (%s)", wait, reason)
        time.sleep(wait)

    def _parse_job_url_direct(self, soup: BeautifulSoup) -> str | None:
        """
        Gets the job url direct from job page
        :param soup:
        :return: str
        """
        job_url_direct = None
        job_url_direct_content = soup.find("code", id="applyUrl")
        if job_url_direct_content:
            job_url_direct_match = self.job_url_direct_regex.search(
                job_url_direct_content.decode_contents().strip()
            )
            if job_url_direct_match:
                job_url_direct = unquote(job_url_direct_match.group())

        return job_url_direct
