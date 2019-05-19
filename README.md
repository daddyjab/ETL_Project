# I Was Poisoned

This application provides Extract/Translate/Load operations to support analysis of Food Poisoning incidents vs. Food Inspections.

# Technologies Used

* Python, Jupyter Notebook, ETL (Extract/Translate/Load)
* MongoDB
* Web Scraping, BeautifulSoup, Splinter

# Reference

* GitHub: https://github.com/daddyjab/IWasPoisoned_ETL
* Web Scraping and Data Exploration (Jupyter Notebook): https://github.com/daddyjab/IWasPoisoned_ETL/blob/master/Extract_IWP_Functions_Illinois_Only.ipynb
* Helper Notebook for Accessing IWP Data in MongoDB: https://github.com/daddyjab/IWasPoisoned_ETL/blob/master/Helper_Accessing_I_Was_Poisoned.ipynb

# Contributions

* Jeffery Brown: Designed and implemented all code for scraping and cleaning content from the [iwaspoisoned.com](https://iwaspoisoned.com) website and storage of this data in a NoSQL database (MongoDB) for later integration with Food Inspections data by another teammember.

* Data:
    * Website documenting self-reported incidences of food poisoning at restaurants: https://iwaspoisoned.com
    * City of Chicago, Department of Health and Human Services, Food Inspections API: https://data.cityofchicago.org/Health-Human-Services/Food-Inspections/4ijn-s7e5

# Summary

Python functions were created to manage scaping of the iwaspoisoned.com website, capturing incident information on each page and across multiple pages:

* `scrape_iwp(a_startpage=1, a_pagecount=20000)`: The function to be called by external programs to perform scraping and provide results in MongoDB.  (This function is a part of our overall ETL pipeline.)

* Supporting functions:

    * `parse_incident_page()`: This function is called for each iwaspoisoned.com website.  It uses the function `parse_one_incident()` to capture the required information for each incident and returns an array of dictionaries.
    * `parse_one_incident()`: This function is called for each incident and captures needed per-incident data, including a call to `get_incident_details()` to scrape incident address information from the per-incident detail page.
    * `get_incident_detail()`: This function is called on a per-incident basis to navigate to the incident detail page to collect information not available from the main page, specifically: address.

| Figure 1: I Was Poisoned - Scraping, Cleaning, and Loading into MongoDB - Jupyter Notebook |
|----------|
| ![Figure 1: I Was Poisoned - Scraping, Cleaning, and Loading into MongoDB - Loading...](docs/IWP-Scraping_Exploration.gif "Figure 1: I Was Poisoned - Scraping, Cleaning, and Loading into MongoDB - Jupyter Notebook") |
