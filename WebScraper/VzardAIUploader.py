import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from seleniumbase import Driver
from selenium.webdriver.common.by import By

from DataProcessing import HTML_OUTPUT_FOLDER, SPLITTED_VIDEO_FOLDER
from WebScraper.ProxyUtil import *
from WebScraper.VideoTranscriptJobDescriptor import *
from WebScraper.WebScrapingUtility import find_element_if_present, click_element_if_clickable, JobStatus, PageUnreachable
from Utility.Logger import Logger

# --- Settings ---
UPLOAD_URL = "https://vizard.ai/upload?from=video-to-text&tool-page=%2Fen%2Ftools%2Fvideo-to-text"
WAIT_TIME_AFTER_UPLOAD = 60 * 10


def MainUploadLoop(driver, language='english', max_retries=3, threadName="Noname"):
    retry_count = 0
    while retry_count < max_retries:
        Logger.info(f"{threadName}: Checking page state (Attempt {retry_count + 1}/{max_retries})...")

        transcript_button = find_element_if_present(driver, By.ID, "transcript_button", 60*3)
        language_xpath = f"//*[translate(normalize-space(text()), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz') = '{language.lower()}']"
        language_button = find_element_if_present(driver, By.XPATH, language_xpath, timeout=5)

        cloudflare_xpath = "//*[contains(translate(normalize-space(text()), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'checking if the site connection is secure')]"
        cloudflare_banner = find_element_if_present(driver, By.XPATH, cloudflare_xpath, timeout=5)

        retry_xpath = "//*[translate(normalize-space(text()), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz') = 'retry']"
        retry_button = find_element_if_present(driver, By.XPATH, retry_xpath, timeout=5)

        if transcript_button:
            Logger.info(f"{threadName}: Found transcript button.")
            if click_element_if_clickable(driver, transcript_button, timeout=30):
                Logger.info(f"{threadName}: Transcript button clicked successfully.")
                break
            else:
                Logger.warning(f"{threadName}: Transcript button found but could not be clicked.")
                retry_count += 1
                time.sleep(5)
                continue

        elif language_button:
            Logger.info(f"{threadName}: Found language button '{language}'.")
            if click_element_if_clickable(driver, language_button, timeout=15):
                Logger.info(f"{threadName}: Language button clicked.")
                continue_xpath = "//*[translate(normalize-space(text()), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz') = 'continue']"
                continue_button = find_element_if_present(driver, By.XPATH, continue_xpath, timeout=5)
                if click_element_if_clickable(driver, continue_button, timeout=5):
                    Logger.info(f"{threadName}: 'Continue' button clicked.")
                    time.sleep(2)
                    continue
                else:
                    Logger.warning(f"{threadName}: 'Continue' button not found or not clickable after selecting language.")
                    retry_count += 1
                    time.sleep(5)
                    continue
            else:
                Logger.warning(f"{threadName}: Could not click the language button.")
                retry_count += 1
                time.sleep(5)
                continue

        elif cloudflare_banner:
            Logger.warning(f"{threadName}: Cloudflare/Bot detection banner detected. Aborting.")
            return

        elif retry_button:
            Logger.info(f"{threadName}: Found 'retry' button.")
            if click_element_if_clickable(driver, retry_button, timeout=10):
                retry_count += 1
                Logger.info(f"{threadName}: Clicked 'retry' button ({retry_count}/{max_retries}).")
                time.sleep(10)
                continue
            else:
                Logger.warning(f"{threadName}: Could not click the 'retry' button.")
                retry_count += 1
                time.sleep(5)
                continue

        else:
            Logger.warning(f"{threadName}: No known state element found. Waiting and retrying...")
            retry_count += 1
            time.sleep(15)

    if retry_count >= max_retries:
        Logger.error(f"{threadName}: Maximum number of retries ({max_retries}) reached. Could not proceed.")


def upload_video(job: VideoTranscriptJobDescriptor, proxy: dict[str] = None, headless_Mode=False):
    job.Lock.acquire()
    if job.IsCompleted:
        job.Lock.release()
        return
    else:
        job.Lock.release()

    threadName = threading.get_ident()
    ConditionSettedByMe = False
    driver = None
    try:
        driver = Driver(uc=True, headless=headless_Mode,
                        proxy=f"{proxy['ip']}:{proxy['port']}" if proxy else None)

        try:
            driver.uc_open_with_reconnect(UPLOAD_URL, 6)
            WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".file-upload-area")))
        except Exception as e:
            Logger.error(f"{threadName}: cannot reach upload page")
            raise PageUnreachable

        file_input = driver.find_element(By.ID, "file-input")
        file_input.send_keys(str(job.VideoPath))

        if job.Lock.locked():
            Logger.info(f"{threadName}: Waiting on lock acquisition")

        job.Lock.acquire()
        ConditionSettedByMe = True
        if job.IsCompleted:
            return
        Logger.info(f"{threadName}: Waiting that the upload finishes...blocking other instances")

        upload_button = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'win-confirm-button') and contains(text(), 'Upload')]"))
        )
        upload_button.click()
        driver.uc_gui_click_captcha()

        MainUploadLoop(driver, job.Language, 3, threadName=str(threadName))

        text_area = driver.find_element(By.ID, "textArea")
        paragraph_count = 0
        while True:
            try:
                paragraph = text_area.find_element(By.ID, f"paragraph_{paragraph_count}")
                driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", paragraph)
                time.sleep(0.5)
                Logger.info(f"{threadName}: Scrolled to paragraph_{paragraph_count}")
                paragraph_count += 1
            except Exception:
                Logger.info(f"{threadName}: No more paragraphs found after paragraph_{paragraph_count-1}. Finished scrolling.")
                break

        Logger.info(f"{threadName}: Scrolling complete.")

        text_area_HTML = text_area.get_attribute("innerHTML")
        html_filename = job.GetHTMLOutputFilePath()
        os.makedirs(os.path.dirname(html_filename), exist_ok=True)
        with open(html_filename, "w", encoding="utf-8") as f:
            f.write(text_area_HTML)
        Logger.info(f"{threadName}: Saved HTML to {html_filename}\n")
        job.IsCompleted = True

    except Exception:
        raise
    finally:
        if driver:
            driver.quit()
        if ConditionSettedByMe:
            job.Lock.release()
            Logger.info(f"{threadName}: lock released for job {job}")


def tryUpload_VzardAi(jobDesc: VideoTranscriptJobDescriptor, proxy: dict[str], headless_mode=False) -> JobStatus:
    try:
        upload_video(jobDesc, proxy, headless_mode)
        return JobStatus.Success
    except PageUnreachable:
        return JobStatus.PageConnectionError
    except Exception as e:
        Logger.error(f"tryUpload_VzardAi Exception: {e}")
        return JobStatus.GenericError



