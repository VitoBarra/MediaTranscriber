import json
import os
import re
import time
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote, urlencode

from seleniumbase import Driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException

from Utility.FileUtil import ReadJson, WriteJson
from Utility.Logger import Logger
from WebScraper.AnyToText import human_delay
from WebScraper.ProxyUtil import getProxyList
from WebScraper.VideoTranscriptJobDescriptor import SharePointLinkJob


def _sanitize_name(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r"[^\w\-]+", "_", name)
    return name[:120] if name else "unnamed"

def load_sharepoint_jobs(raw_folder: Path | str) -> list[SharePointLinkJob]:
    raw_folder = Path(raw_folder)

    candidates = sorted(raw_folder.glob("*.json"))
    if not candidates:
        raise FileNotFoundError(f"No json found in {raw_folder}")

    Logger.info(f"SharePoint: loading {len(candidates)} json file(s)")

    jobs: list[SharePointLinkJob] = []
    name_to_source: dict[str, Path] = {}

    for data_path in candidates:
        Logger.info(f"Reading SharePoint links: {data_path}")

        txt = data_path.read_text(encoding="utf-8-sig", errors="strict").strip()
        if not txt:
            raise ValueError(f"SharePoint links file is empty: {data_path}")

        try:
            data = json.loads(txt)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in {data_path}: {e}") from e

        def add_job(name: str, url: str):
            clean_name = _sanitize_name(str(name))

            if clean_name in name_to_source:
                other = name_to_source[clean_name]
                raise ValueError(
                    f"Duplicate SharePoint job name '{clean_name}' "
                    f"found in:\n - {other}\n - {data_path}"
                )

            name_to_source[clean_name] = data_path
            jobs.append(SharePointLinkJob(clean_name, str(url)))

        if isinstance(data, dict):
            for k, v in data.items():
                add_job(k, v)

        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and "name" in item and "url" in item:
                    add_job(item["name"], item["url"])

                elif isinstance(item, (list, tuple)) and len(item) >= 2:
                    add_job(item[0], item[1])

                else:
                    raise ValueError(f"Unsupported item in {data_path}: {item!r}")

        else:
            raise ValueError(f"Unsupported json format in {data_path}")

    Logger.info(f"Loaded {len(jobs)} SharePoint link jobs (no duplicates)")
    return jobs



def get_profile_dir(profile_name) -> str:
    base = os.path.join(os.environ.get("LOCALAPPDATA", os.path.expanduser("~")), "SeleniumProfiles")
    path = os.path.join(base, profile_name)
    os.makedirs(path, exist_ok=True)
    return path


def normalize_sharepoint_stream_url(url: str) -> str:
    p = urlparse(url)
    qs = parse_qs(p.query)

    if "id" in qs and qs["id"]:
        decoded = qs["id"][0]
        for _ in range(3):
            d2 = unquote(decoded)
            if d2 == decoded:
                break
            decoded = d2
        qs["id"] = [decoded]

    flat = {k: (v[0] if isinstance(v, list) and v else v) for k, v in qs.items()}
    new_query = urlencode(flat, doseq=False, safe="/%:.-_")
    return p._replace(query=new_query).geturl()


def _safe_text(el) -> str:
    try:
        return (el.text or "").strip()
    except Exception:
        return ""


def _extract_int(s: str) -> int | None:
    m = re.search(r"(\d+)", s or "")
    return int(m.group(1)) if m else None


def _find_transcript_scroll_container(driver):
    rows = driver.find_elements(By.CSS_SELECTOR, "div[id^='sub-entry-']")
    if not rows:
        return None

    el = rows[0]
    for _ in range(35):
        try:
            parent = el.find_element(By.XPATH, "..")
        except Exception:
            return None

        is_scrollable = driver.execute_script("""
            const e = arguments[0];
            if (!e) return false;
            const style = window.getComputedStyle(e);
            const oy = style.overflowY;
            return (oy === 'auto' || oy === 'scroll') && (e.scrollHeight > e.clientHeight + 5);
        """, parent)

        if is_scrollable:
            return parent

        el = parent

    return None


def _parse_transcript_item(driver, idx: int) -> dict | None:
    try:
        sub = driver.find_elements(By.CSS_SELECTOR, f"div#sub-entry-{idx}")
        if not sub:
            return None

        text = _safe_text(sub[0])
        if not text:
            return None

        speaker = ""
        timestamp = ""

        header = driver.find_elements(By.CSS_SELECTOR, f"div#itemHeader-{idx} span.itemDisplayName-507")
        if header:
            speaker = _safe_text(header[0])

        ts = driver.find_elements(By.CSS_SELECTOR, f"span#Header-timestamp-{idx}")
        if ts:
            timestamp = _safe_text(ts[0])

        if not speaker:
            ev_sp = driver.find_elements(By.CSS_SELECTOR, f"div#sub-entry-{idx} p.eventSpeakerName-501")
            if ev_sp:
                speaker = _safe_text(ev_sp[0])

        return {"index": idx, "timestamp": timestamp, "speaker": speaker, "text": text}

    except StaleElementReferenceException:
        return None


def open_transcript_panel_if_needed(driver):
    if driver.find_elements(By.CSS_SELECTOR, "div[id^='sub-entry-']"):
        return

    candidates = [
        "xpath=//button[contains(., 'Transcript')]",
        "xpath=//button[contains(., 'Trascrizione')]",
        "xpath=//*[self::button or self::a][contains(., 'Transcript')]",
        "xpath=//*[self::button or self::a][contains(., 'Trascrizione')]",
    ]
    for sel in candidates:
        try:
            driver.click(sel)
            human_delay(0.8, 1.5)
            if driver.find_elements(By.CSS_SELECTOR, "div[id^='sub-entry-']"):
                return
        except Exception:
            pass


def read_full_rows(
    driver,
    timeout=30,
    step_ratio=0.75,
    pause=0.40,
    max_loops=20000,
    stable_loops=180,
    max_seconds=45 * 60,
):
    wait = WebDriverWait(driver, timeout)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[id^='sub-entry-']")))

    scroll_container = _find_transcript_scroll_container(driver)
    use_window = scroll_container is None

    seen: dict[int, dict] = {}
    prev_count = 0
    stable = 0
    t0 = time.time()

    for loop_i in range(max_loops):
        if time.time() - t0 > max_seconds:
            Logger.warning("SharePoint: max_seconds reached, stopping.")
            break

        ids = driver.execute_script("""
            return Array.from(document.querySelectorAll("div[id^='sub-entry-']"))
                .map(e => e.id);
        """) or []

        indices = []
        for sid in ids:
            n = _extract_int(sid)
            if n is not None:
                indices.append(n)

        for idx in sorted(set(indices)):
            if idx in seen:
                continue
            item = _parse_transcript_item(driver, idx)
            if item:
                seen[idx] = item

        if len(seen) == prev_count:
            stable += 1
        else:
            stable = 0
            prev_count = len(seen)

        if stable >= stable_loops:
            break

        if loop_i % 80 == 0:
            Logger.info(f"SharePoint scroll: loops={loop_i} rows={len(seen)} stable={stable}")

        if use_window:
            step = driver.execute_script("return Math.floor(window.innerHeight * arguments[0]);", step_ratio)
            driver.execute_script("window.scrollBy(0, arguments[0]);", step)
        else:
            step = driver.execute_script(
                "return Math.max(80, Math.floor(arguments[0].clientHeight * arguments[1]));",
                scroll_container,
                step_ratio,
            )
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + arguments[1];", scroll_container, step)

        time.sleep(pause)

    return [seen[i] for i in sorted(seen.keys())]


def rows_to_text(rows: list[dict]) -> str:
    return "\n".join((r.get("text") or "").strip() for r in rows if (r.get("text") or "").strip())


# ----------------------------
# Public functions
# ----------------------------
def GetTranscriptDataFromSharedPoint(
    *,
    headless: bool,
    raw_links_folder: Path | str,
    json_out_folder: Path | str,
    profile_name: str = "shared_point_login",
    proxy_file: Path | str | None = None,
    max_proxy_age_seconds: int = 1800,
    wait_login_seconds: int = 600,
    skip_if_exists: bool = True,
):

    raw_links_folder = Path(raw_links_folder)
    json_out_folder = Path(json_out_folder)
    json_out_folder.mkdir(parents=True, exist_ok=True)

    jobs = load_sharepoint_jobs(raw_links_folder)

    proxy = None
    proxy_str = None
    if proxy_file:
        proxy_list = getProxyList(proxy_file, max_proxy_age_seconds)
        proxy = proxy_list[0] if proxy_list else None
        proxy_str = f"{proxy['ip']}:{proxy['port']}" if proxy else None

    profile_dir = get_profile_dir(profile_name)
    Logger.info(f"SharePoint: profile={profile_dir} headless={headless} proxy={proxy_str}")
    Logger.info(f"SharePoint: json_out_folder={json_out_folder}")

    driver = Driver(
        uc=True,
        headless=headless,
        user_data_dir=profile_dir,
        chromium_arg="--profile-directory=Default",
        proxy=proxy_str,
    )

    try:
        for job in jobs:
            name = _sanitize_name(job.name)
            url = normalize_sharepoint_stream_url(job.url)
            out_json = json_out_folder / f"{name}.json"

            if skip_if_exists and out_json.exists():
                Logger.info(f"{name}: json exists, skipping.")
                continue

            Logger.info(f"{name}: opening {url}")
            driver.get(url)
            human_delay(3.2, 5.2)

            open_transcript_panel_if_needed(driver)

            Logger.info(f"{name}: wait transcript (login if needed)")
            WebDriverWait(driver, wait_login_seconds).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[id^='sub-entry-']"))
            )

            Logger.info(f"{name}: scrolling transcript")
            rows = read_full_rows(driver)

            WriteJson(out_json, {"name": name, "url": url, "rows": rows})
            Logger.info(f"{name}: saved json -> {out_json} (rows={len(rows)})")

            time.sleep(2.0)

    except TimeoutException as e:
        Logger.error(f"SharePoint TIMEOUT: {e}")
        raise
    finally:
        try:
            driver.quit()
        except Exception:
            pass
        Logger.info("SharePoint: Driver closed.")


def ConvertSharePointJsonToMarkdown(
    json_folder: Path | str,
    md_out_folder: Path | str,
):
    """
    Convert previously saved SharePoint JSON files into concatenated transcript .md.

    Output per video:
      <md_out_folder>/<name>.md   (concatenated text only)
    """
    json_folder = Path(json_folder)
    md_out_folder = Path(md_out_folder)

    json_files = sorted(json_folder.glob("*.json"))
    if not json_files:
        Logger.warning(f"SharePoint: no json files found in {json_folder}")
        return

    Logger.info(f"SharePoint: converting {len(json_files)} json files -> md in {md_out_folder}")

    for p in json_files:
        try:
            data = ReadJson(p)  # your helper
            name = _sanitize_name(str(data.get("name") or p.stem))
            rows = data.get("rows") or []
            out_md = md_out_folder / f"{name}.md"

            if out_md.exists():
                Logger.info(f"{name}: md exists, skipping.")
                continue

            text = rows_to_text(rows)
            out_md.write_text(text, encoding="utf-8")
            Logger.info(f"{name}: saved md -> {out_md} (chars={len(text)})")

        except Exception as e:
            Logger.error(f"SharePoint: convert failed for {p}: {e}")



