"""
career_radar.jobboard.exceptions
~~~~~~~~~~~~~~~~~~~

This module contains the set of Scrapers' exceptions.
"""


import warnings


class ScraperWarning(UserWarning):
    """Warning emitted when a scraper returns fewer results than requested, likely due to rate limiting."""
    pass


class ScraperUnavailableError(Exception):
    """Exception raised when a scraper's endpoint is unavailable or needs update."""
    def __init__(self, message=None, site=None):
        self.site = site
        super().__init__(message or f"Scraper for {site} is currently unavailable. Endpoint may need update.")


class LinkedInException(Exception):
    def __init__(self, message=None):
        super().__init__(message or "An error occurred with LinkedIn")


class IndeedException(Exception):
    def __init__(self, message=None):
        super().__init__(message or "An error occurred with Indeed")


class ZipRecruiterException(Exception):
    def __init__(self, message=None):
        super().__init__(message or "An error occurred with ZipRecruiter")


class GlassdoorException(Exception):
    def __init__(self, message=None):
        super().__init__(message or "An error occurred with Glassdoor")


class GoogleJobsException(Exception):
    def __init__(self, message=None):
        super().__init__(message or "An error occurred with Google Jobs")


class BaytException(Exception):
    def __init__(self, message=None):
        super().__init__(message or "An error occurred with Bayt")

class NaukriException(Exception):
    def __init__(self,message=None):
        super().__init__(message or "An error occurred with Naukri")


class BDJobsException(Exception):
    def __init__(self, message=None):
        super().__init__(message or "An error occurred with BDJobs")


class InternshalaException(Exception):
    def __init__(self, message=None):
        super().__init__(message or "An error occurred with Internshala")


class FounditException(Exception):
    def __init__(self, message=None):
        super().__init__(message or "An error occurred with Foundit")


class ShineException(Exception):
    def __init__(self, message=None):
        super().__init__(message or "An error occurred with Shine")


class TimesJobsException(Exception):
    def __init__(self, message=None):
        super().__init__(message or "An error occurred with TimesJobs")