from __future__ import annotations

import hashlib
import math
import random
import time
from datetime import date, datetime, timedelta
from typing import Optional

import regex as re
from bs4 import BeautifulSoup

from career_radar.model import (
    Compensation,
    CompensationInterval,
    Country,
    JobPost,
    JobResponse,
    JobType,
    Location,
    Scraper,
    ScraperInput,
    Site,
)
from career_radar.util import create_logger, create_session, extract_emails_from_text

log = create_logger("Internshala")


class Internshala(Scraper):
    """Production-hardened Internshala scraper with resilient parsing and retries."""

    base_url = "https://internshala.com"
    jobs_per_page = 20
    max_retries = 4
    transient_statuses = {408, 425, 429, 500, 502, 503, 504, 520, 522, 524}

    def __init__(
        self,
        proxies: list[str] | str | None = None,
        ca_cert: str | None = None,
        user_agent: str | None = None,
    ):
        super().__init__(Site.INTERNSHALA, proxies=proxies, ca_cert=ca_cert, user_agent=user_agent)
        self.session = create_session(
            proxies=self.proxies,
            ca_cert=ca_cert,
            is_tls=False,
            has_retry=True,
            delay=2,
            clear_cookies=False,
            user_agent=user_agent,
        )
        self.session.headers.update(
            {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "accept-language": "en-US,en;q=0.9",
                "cache-control": "no-cache",
                "referer": self.base_url,
            }
        )
        self.scraper_input: ScraperInput | None = None
        self.country = Country.INDIA

    def scrape(self, scraper_input: ScraperInput) -> JobResponse:
        """Scrapes Internshala list pages and returns normalized job posts."""
        self.scraper_input = scraper_input
        seen_ids: set[str] = set()
        job_list: list[JobPost] = []
        page = 1
        empty_page_streak = 0
        target_count = scraper_input.results_wanted + scraper_input.offset
        max_pages = max(5, min(50, math.ceil(target_count / self.jobs_per_page) + 5))

        while len(job_list) < target_count and page <= max_pages:
            log.info(
                "search page: %s / %s",
                page,
                max(1, math.ceil(scraper_input.results_wanted / self.jobs_per_page)),
            )
            url = self._build_url(scraper_input, page)
            soup = self._fetch_page(url)
            if soup is None:
                break

            cards = self._extract_job_cards(soup)
            if not cards:
                empty_page_streak += 1
                if empty_page_streak >= 2:
                    break
                page += 1
                continue

            empty_page_streak = 0
            for card in cards:
                job_id = self._get_job_id(card)
                if not job_id or job_id in seen_ids:
                    continue
                seen_ids.add(job_id)

                try:
                    job_post = self._process_job_card(card, job_id)
                except Exception as exc:
                    log.warning("Internshala card parse failed (%s): %s", job_id, exc)
                    continue

                if job_post is not None:
                    job_list.append(job_post)
                if len(job_list) >= target_count:
                    break

            if len(job_list) < target_count:
                time.sleep(self._get_delay_seconds())
            page += 1

        start = scraper_input.offset
        end = start + scraper_input.results_wanted
        return JobResponse(jobs=job_list[start:end])

    def _build_url(self, scraper_input: ScraperInput, page: int) -> str:
        """Builds Internshala listing URL from search/location input."""
        keyword_slug = self._slugify(scraper_input.search_term or "")
        location_slug = self._slugify(scraper_input.location or "")
        path = "/jobs/"

        if keyword_slug and location_slug:
            path = f"/jobs/{keyword_slug}-jobs-in-{location_slug}/"
        elif keyword_slug:
            path = f"/jobs/{keyword_slug}-jobs/"
        elif location_slug:
            path = f"/jobs/jobs-in-{location_slug}/"

        if page > 1:
            path = path.rstrip("/") + f"/page-{page}/"

        return f"{self.base_url}{path}"

    def _slugify(self, text: str) -> str:
        """Converts arbitrary text into URL slug format expected by Internshala."""
        value = str(text or "").strip().lower()
        if not value:
            return ""
        value = re.sub(r"[^a-z0-9\s-]", " ", value)
        value = re.sub(r"\s+", "-", value).strip("-")
        value = re.sub(r"-{2,}", "-", value)
        return value

    def _fetch_page(self, url: str) -> BeautifulSoup | None:
        """Fetches one Internshala page with retry/backoff."""
        timeout = max(10, int(self.scraper_input.request_timeout or 10))
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(url, timeout=timeout)
            except Exception as exc:
                if attempt == self.max_retries:
                    log.error("Internshala request failed: %s", exc)
                    return None
                self._retry_sleep(attempt, "request exception")
                continue

            if response.status_code in self.transient_statuses:
                if attempt == self.max_retries:
                    log.warning("Internshala transient status persisted: %s", response.status_code)
                    return None
                self._retry_sleep(attempt, f"status={response.status_code}")
                continue

            if response.status_code not in range(200, 400):
                log.warning("Internshala non-success status: %s", response.status_code)
                return None

            text_lower = response.text.lower()
            challenge_markers = (
                "verify you are a human",
                "checking your browser before accessing",
                "cf-challenge",
                "hcaptcha",
                "g-recaptcha",
                "access denied",
            )
            if any(marker in text_lower for marker in challenge_markers):
                log.warning("Internshala challenge detected; returning partial results.")
                return None

            return BeautifulSoup(response.text, "html.parser")
        return None

    def _retry_sleep(self, attempt: int, reason: str) -> None:
        """Backoff with jitter for transient Internshala failures."""
        base = [1.5, 3.0, 6.0, 10.0]
        idx = min(attempt - 1, len(base) - 1)
        wait_for = base[idx] + random.uniform(0.2, 0.9)
        log.warning("Internshala retry in %.1fs (%s)", wait_for, reason)
        time.sleep(wait_for)

    def _get_delay_seconds(self) -> float:
        """Respects configured delay_between_requests_ms to reduce blocking risk."""
        delay_config = self.scraper_input.delay_between_requests_ms
        if isinstance(delay_config, tuple):
            min_ms, max_ms = delay_config
            return max(0.5, random.uniform(min_ms, max_ms) / 1000.0)
        return max(0.8, float(delay_config) / 1000.0)

    def _extract_job_cards(self, soup: BeautifulSoup) -> list:
        """Extracts list-card nodes using fallback selectors for markup drift."""
        selectors = [
            ".individual_internship",
            ".container-fluid.individual_internship",
            "[data-internship-id]",
            "[data-job-id]",
            ".internship_list_container .individual_internship",
        ]
        for selector in selectors:
            cards = soup.select(selector)
            if cards:
                return cards
        return []

    def _get_job_id(self, card) -> str | None:
        """Derives a stable ID from attributes, URL, or content fallback."""
        for key in ("data-internship-id", "data-job-id", "id"):
            value = card.get(key)
            if value:
                return str(value)

        link = card.select_one("a[href*='/job/detail/'], a[href*='/internship/detail/'], a.job-title-href")
        if link and link.get("href"):
            href = link.get("href", "")
            match = re.search(r"(\d{8,})", href)
            if match:
                return match.group(1)
            digest = hashlib.md5(href.encode("utf-8")).hexdigest()[:12]
            return f"url-{digest}"

        content = card.get_text(" ", strip=True)
        if content:
            digest = hashlib.md5(content.encode("utf-8")).hexdigest()[:12]
            return f"txt-{digest}"
        return None

    def _process_job_card(self, card, job_id: str) -> Optional[JobPost]:
        """Parses one Internshala listing card into JobPost."""
        title_elem = card.select_one(
            "a.job-title-href, h3 a, .heading_4_5 a, .job-internship-name a, .profile a"
        )
        title = title_elem.get_text(" ", strip=True) if title_elem else None
        if not title:
            title = self._extract_text(card, [".heading_4_5", "h3", ".profile"])
        if not title:
            return None

        job_url = self._extract_job_url(card, title_elem, job_id)
        company_name = self._extract_text(
            card,
            [".company_name", ".link_display_like_text", ".company-name", "p.company_name a", ".heading_6"],
        )
        location = self._parse_location(card)

        salary_text = self._extract_salary_text(card)
        compensation = self._parse_compensation(salary_text) if salary_text else None
        date_posted = self._parse_date(card)
        skills = self._parse_skills(card)
        is_remote = self._check_remote(card, title, location)
        is_internship = self._check_is_internship(card, job_url)
        job_type = self._infer_job_type(card, title, is_internship)
        apply_by = self._parse_apply_by(card)
        experience_range = self._parse_experience(card)
        description = self._extract_text(
            card,
            [".internship_other_details_container", ".detail_view", ".job_description", ".other_detail_item"],
        )

        if self.scraper_input.is_remote and not is_remote:
            return None
        if self.scraper_input.job_type and (not job_type or self.scraper_input.job_type not in job_type):
            return None

        return JobPost(
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
            stipend=salary_text,
            apply_by=apply_by,
        )

    def _extract_job_url(self, card, title_elem, job_id: str) -> str:
        """Gets absolute listing URL from preferred anchors."""
        candidate = None
        if title_elem is not None and title_elem.get("href"):
            candidate = title_elem.get("href")
        if not candidate:
            link = card.select_one("a[href*='/job/detail/'], a[href*='/internship/detail/'], a[href]")
            candidate = link.get("href") if link else None
        if candidate:
            return candidate if candidate.startswith("http") else f"{self.base_url}{candidate}"
        return f"{self.base_url}/job/detail/{job_id}"

    def _extract_text(self, card, selectors: list[str]) -> str | None:
        """Returns first non-empty text for a list of CSS selectors."""
        for selector in selectors:
            elem = card.select_one(selector)
            if elem:
                text = elem.get_text(" ", strip=True)
                if text:
                    return text
        return None

    def _parse_location(self, card) -> Location:
        """Parses city/state style location from card content."""
        loc_text = self._extract_text(
            card,
            [
                ".location_link",
                ".individual_location_name",
                "#location_names a",
                "#location_names span",
                ".locations a",
                ".ic-16-map-pin + span",
            ],
        )
        if not loc_text:
            card_text = card.get_text(" ", strip=True)
            if "work from home" in card_text.lower() or "remote" in card_text.lower():
                return Location(city="Remote", country=Country.INDIA)
            return Location(country=Country.INDIA)

        parts = [p.strip() for p in loc_text.split(",") if p.strip()]
        city = parts[0] if parts else None
        state = parts[1] if len(parts) > 1 else None
        return Location(city=city, state=state, country=Country.INDIA)

    def _extract_salary_text(self, card) -> str | None:
        """Extracts salary/stipend text from likely compensation fields."""
        text = self._extract_text(
            card,
            [
                ".salary",
                ".stipend",
                ".ic-16-money + span",
                ".item_body",
                "span.desktop-text",
            ],
        )
        if not text:
            return None
        lower = text.lower()
        if any(token in lower for token in ("stipend", "salary", "lpa", "lakh", "month", "year", "annum", "₹", "inr", "rs")):
            return text
        return None

    def _parse_compensation(self, salary_text: str) -> Optional[Compensation]:
        """Parses common salary text formats into normalized compensation."""
        if not salary_text:
            return None
        normalized = salary_text.replace("Rs.", "INR").replace("₹", "INR").replace("–", "-").replace("—", "-")
        lower = normalized.lower()
        if any(token in lower for token in ("unpaid", "not disclosed", "n/a")):
            return None

        lpa_range = re.search(r"(\d+(?:\.\d+)?)\s*(?:-|to)\s*(\d+(?:\.\d+)?)\s*(?:lpa|lakh|lacs?)", lower)
        if lpa_range:
            min_val = float(lpa_range.group(1)) * 100000
            max_val = float(lpa_range.group(2)) * 100000
            return Compensation(
                min_amount=int(min_val),
                max_amount=int(max_val),
                currency="INR",
                interval=CompensationInterval.YEARLY,
            )

        lpa_single = re.search(r"(\d+(?:\.\d+)?)\s*(?:lpa|lakh|lacs?)", lower)
        if lpa_single:
            amount = float(lpa_single.group(1)) * 100000
            return Compensation(
                min_amount=int(amount),
                max_amount=int(amount),
                currency="INR",
                interval=CompensationInterval.YEARLY,
            )

        inr_range = re.search(r"(?:inr|rs\.?)?\s*([\d,]+)\s*(?:-|to)\s*(?:inr|rs\.?)?\s*([\d,]+)", lower)
        if inr_range:
            min_val = int(inr_range.group(1).replace(",", ""))
            max_val = int(inr_range.group(2).replace(",", ""))
            interval = CompensationInterval.MONTHLY if "month" in lower else CompensationInterval.YEARLY
            return Compensation(min_amount=min_val, max_amount=max_val, currency="INR", interval=interval)

        inr_single = re.search(r"(?:inr|rs\.?)\s*([\d,]+)", lower)
        if inr_single:
            amount = int(inr_single.group(1).replace(",", ""))
            interval = CompensationInterval.MONTHLY if "month" in lower else CompensationInterval.YEARLY
            return Compensation(min_amount=amount, max_amount=amount, currency="INR", interval=interval)

        return None

    def _parse_date(self, card) -> Optional[date]:
        """Parses relative posted date text into concrete date."""
        candidates = [
            self._extract_text(
                card,
                [
                    ".date",
                    ".posted_by_container span",
                    ".status-success",
                    ".status-info",
                    ".ic-16-calendar + span",
                ],
            ),
            card.get_text(" ", strip=True),
        ]
        today = datetime.now().date()

        for value in candidates:
            if not value:
                continue
            text = value.lower()
            if "just now" in text or "today" in text:
                return today
            if "yesterday" in text:
                return today - timedelta(days=1)

            day_match = re.search(r"(\d+)\s*day", text)
            if day_match:
                return today - timedelta(days=int(day_match.group(1)))

            week_match = re.search(r"(\d+)\s*week", text)
            if week_match:
                return today - timedelta(weeks=int(week_match.group(1)))

            month_match = re.search(r"(\d+)\s*month", text)
            if month_match:
                return today - timedelta(days=30 * int(month_match.group(1)))

            hour_match = re.search(r"(\d+)\s*hour", text)
            if hour_match:
                return today

        return None

    def _parse_skills(self, card) -> list[str] | None:
        """Extracts and deduplicates listed skills."""
        skill_elems = card.select(
            ".round_tabs, .skill_tag, .individual_skill, .tags .tag, .individual_internship_tag"
        )
        if not skill_elems:
            return None
        seen: set[str] = set()
        ordered: list[str] = []
        for elem in skill_elems:
            skill = elem.get_text(" ", strip=True)
            if skill and skill.lower() not in seen:
                seen.add(skill.lower())
                ordered.append(skill)
        return ordered or None

    def _check_remote(self, card, title: str, location: Location) -> bool:
        """Checks remote indicators across card text/title/location."""
        remote_keywords = ("remote", "work from home", "wfh")
        card_text = card.get_text(" ", strip=True).lower()
        title_text = (title or "").lower()
        location_text = location.display_location().lower()
        return any(
            token in f"{card_text} {title_text} {location_text}" for token in remote_keywords
        )

    def _check_is_internship(self, card, job_url: str) -> bool:
        """Determines if role is internship based on URL and card text."""
        url_lower = (job_url or "").lower()
        if "/internship/" in url_lower:
            return True
        card_text = card.get_text(" ", strip=True).lower()
        return "internship" in card_text

    def _infer_job_type(self, card, title: str, is_internship: bool) -> list[JobType] | None:
        """Infers JobType list from listing text."""
        if is_internship:
            return [JobType.INTERNSHIP]

        text = f"{card.get_text(' ', strip=True)} {title or ''}".lower()
        detected: list[JobType] = []
        if "full time" in text or "full-time" in text:
            detected.append(JobType.FULL_TIME)
        if "part time" in text or "part-time" in text:
            detected.append(JobType.PART_TIME)
        if "contract" in text:
            detected.append(JobType.CONTRACT)
        if "intern" in text:
            detected.append(JobType.INTERNSHIP)
        return detected or [JobType.FULL_TIME]

    def _parse_apply_by(self, card) -> str | None:
        """Extracts apply-by date phrase from card text."""
        text = card.get_text(" ", strip=True)
        match = re.search(r"apply\s*by\s*[:\-]?\s*([a-z0-9,\s-]+)", text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return None

    def _parse_experience(self, card) -> str | None:
        """Extracts experience signal such as fresher or year ranges."""
        text = card.get_text(" ", strip=True).lower()
        fresher_match = re.search(r"(fresher|no experience|0\s*[-to]\s*\d+\s*(?:year|yr)s?)", text, re.IGNORECASE)
        if fresher_match:
            return fresher_match.group(1)
        range_match = re.search(r"(\d+\s*[-to]\s*\d+\s*(?:year|yr)s?)", text, re.IGNORECASE)
        if range_match:
            return range_match.group(1)
        plus_match = re.search(r"(\d+\+\s*(?:year|yr)s?)", text, re.IGNORECASE)
        if plus_match:
            return plus_match.group(1)
        return None
