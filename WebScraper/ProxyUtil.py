import ipaddress
import requests
from requests.exceptions import RequestException
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from swiftshadow.classes import ProxyInterface

from Utility.FileUtil import ReadJson, WriteJson, IsModifiedRecently
from WebScraper import PROXY_FILE
from Utility.Logger import Logger


def test_proxy(ip, port, test_url="https://httpbin.org/ip", use_https=False):
    proxy = f"{ip}:{port}"
    proxies = {"http": f"https://{proxy}"} if use_https else {"http": f"http://{proxy}"}

    try:
        response = requests.get(test_url, proxies=proxies, timeout=5)
        if response.status_code == 200:
            Logger.info(f"Working proxy: {proxy}")
            return True
        else:
            Logger.warning(f"Bad response from proxy {proxy}: {response.status_code}")
    except RequestException as e:
        Logger.error(f"Proxy failed: {proxy} – {e}")

    return False


def is_valid_ip(ip):
    try:
        ipaddress.ip_address(ip)
        return True
    except ValueError:
        return False


def fetch_proxies():
    url = "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all"
    try:
        response = requests.get(url)
        response.raise_for_status()
    except RequestException as e:
        Logger.error(f"Failed to fetch proxies from ProxyScrape: {e}")
        return []

    if not response.text.strip():
        Logger.warning("No proxies found from ProxyScrape.")
        return []

    proxy_list = response.text.strip().split('\r\n')
    proxy_obj = [{"ip": ip, "port": port} for ip, port in (proxy.split(':') for proxy in proxy_list)]
    proxy_obj = [p for p in proxy_obj if is_valid_ip(p["ip"])]
    Logger.info(f"Fetched {len(proxy_obj)} valid proxies from ProxyScrape.")
    return proxy_obj


def fetchHTTPS_proxies():
    proxies = []

    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')

    Logger.info("Fetching HTTPS proxies from sslproxies.org...")
    driver = webdriver.Chrome(options=options)
    driver.get('https://www.sslproxies.org/')

    rows = driver.find_elements(By.CSS_SELECTOR, 'table.table tbody tr')
    Logger.info(f"Found {len(rows)} proxies in the table.")

    for row in rows:
        columns = row.find_elements(By.TAG_NAME, 'td')
        if len(columns) >= 2:
            proxies.append({'ip': columns[0].text, 'port': columns[1].text})

    driver.quit()
    filtered_proxies = [proxy for proxy in proxies if is_valid_ip(proxy["ip"])]
    Logger.info(f"{len(filtered_proxies)} valid HTTPS proxies extracted.")
    return filtered_proxies


def fetch_proxy_swiftshadow(find_https=True):
    proxy_manager = ProxyInterface(
        countries=[],
        protocol="https" if find_https else "http",
        autoRotate=True,
    )

    proxy_list = []
    for _ in range(100):
        proxy = proxy_manager.get()
        if proxy is None:
            Logger.warning("Couldn't find a proxy from SwiftShadow.")
            continue
        proxy_list.append({"ip": proxy.ip, "port": proxy.port})
    Logger.info(f"Fetched {len(proxy_list)} proxies from SwiftShadow.")
    return proxy_list


def getProxyList(proxy_file=PROXY_FILE, MAX_AGE_SECONDS=1800):
    if proxy_file is None:
        return None

    if IsModifiedRecently(proxy_file, MAX_AGE_SECONDS):
        proxy_list = ReadJson(proxy_file)
        Logger.info(f"Loaded {len(proxy_list)} proxies from file '{proxy_file}'")
        if proxy_list:
            return proxy_list

    Logger.info("Finding new proxies...")
    proxy_list = fetch_proxies()
    WriteJson(proxy_file, proxy_list)
    Logger.info(f"Downloaded {len(proxy_list)} proxies and saved to file '{proxy_file}'")

    return proxy_list
