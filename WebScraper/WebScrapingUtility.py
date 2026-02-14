import random
import time
from enum import Enum
from selenium.common import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from Utility.Logger import Logger


class JobStatus(Enum):
    Success = 0
    PageConnectionError = 1
    GenericError = 2


class PageUnreachable(BaseException):
    """Raised when a web page cannot be reached."""
    pass


def find_element_if_present(driver, by, value, timeout=1):
    """
    Find an element within the given timeout.

    Args:
        driver: Selenium WebDriver instance.
        by: Locator strategy (By.ID, By.CSS_SELECTOR, etc.).
        value: Locator value.
        timeout: Maximum wait time in seconds.

    Returns:
        WebElement if found, otherwise None.
    """
    try:
        element = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((by, value))
        )
        Logger.debug(f"Element found: {value}")
        return element
    except TimeoutException:
        Logger.debug(f"Element not found within {timeout}s: {value}")
        return None
    except Exception as e:
        Logger.error(f"Unexpected error finding element {value}: {e}")
        return None


def click_element_if_clickable(driver, element, timeout=5, threadName="noname"):
    """
    Click an element if it becomes clickable within the timeout.

    Args:
        driver: Selenium WebDriver instance.
        element: WebElement to click.
        timeout: Maximum wait time in seconds.
        threadName: Optional identifier for logging.

    Returns:
        True if clicked successfully, False otherwise.
    """
    if not element:
        Logger.debug(f"{threadName}: No element provided to click.")
        return False
    try:
        clickable_element = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable(element)
        )
        clickable_element.click()
        Logger.info(f"{threadName}: Element clicked successfully.")
        return True
    except TimeoutException:
        Logger.warning(f"{threadName}: Element found but not clickable within {timeout}s.")
        return False
    except Exception as e:
        Logger.error(f"{threadName}: Error during click: {e}")
        return False

def human_delay(a=1.4, b=2.8):
    time.sleep(random.uniform(a, b))