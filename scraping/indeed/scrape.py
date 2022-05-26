# exec(open("scrape.py").read())
# python scrape.py
# A script to scrape basic job information from Indeed
import subprocess as sp
import requests
import random
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import hashlib
import time
import random

def getSoup(url: str, driver) -> BeautifulSoup:
    driver.get(url)
    return BeautifulSoup(driver.page_source, "html.parser")

def cookSoup(soup: BeautifulSoup) -> list:
    # id = "resultscol" is the primary parent that contains all job data and
    # pagination links
    results = soup.find(id = "resultsCol")

    # id = "mosaic-provider-jobcards" is the parent of <ul
    # class="jobsearch-resultsList"> that contains the list of jobs. 
    joblist = list(soup.find(id = "mosaic-provider-jobcards")\
        .find("ul", class_ = "jobsearch-ResultsList")\
        .findChildren("li", recursive = False))

    # Extract job data.
    ret = dict()
    for job in joblist:
        jobdata = job.find("table", class_ = "jobCard_mainContent big6_visualChanges")
        if jobdata is None:
            continue
        resultContent = jobdata.find("td", class_ = "resultContent")
        jobTitleTag = resultContent.find("h2", class_ = "jobTitle")
        jobTitle = jobTitleTag.text
        link = "https://ca.indeed.com" +\
            jobTitleTag.findChildren("a", recursive = "false")[0]["href"]
        if jobTitle[0:3] == "new":
            jobTitle = jobTitle[3:]
        companyName = resultContent.find("span", class_ = "companyName").text
        companyLocation = resultContent.find("div", class_ = "companyLocation").text
        ls = [jobTitle, companyName, companyLocation, link]
        m = hashlib.sha256()
        m.update(b"".join(list(map(lambda x: x.encode("utf-8"), ls))))
        hash = m.hexdigest()
        ret[hash] = {"jobTitle": jobTitle, "companyName": companyName
            ,"companyLocation": companyLocation, "link": link, "hash": hash}
    return ret

if __name__ == "__main__":
    sp.call("cls", shell = True)

    hashKeys = set()
    cols = ["jobTitle", "companyName", "companyLocation", "link", "hash"]
    with webdriver.Chrome() as driver:
        for start in range(0, 900, 10):
            url = "https://ca.indeed.com/jobs?q=developer&l=Canada&sort=date&filter=0" + f"&start={start}"
            soup = getSoup(url, driver)
            cooked = cookSoup(soup)

            # Write to csv
            with open(f"csv\\{start}.csv", "wt") as output:
                for dct in cooked.values():
                    output.write(",".join([f"\"{dct[x]}\"" for x in cols]) + "\n")
            keys = set(cooked.keys())
            subset = keys.issubset(hashKeys)
            if not(subset):
                hashKeys.update(keys)
                ts = 1 + random.randint(1,3) + random.random()
                print(f"Scraped start = {start}. {subset}. Sleeping for {ts}s")
                time.sleep(ts)
            else:
                break