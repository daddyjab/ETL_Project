#!/usr/bin/env python
# coding: utf-8

# PROJECT ETL
#@Author: Jeff Brown (daddyjab)<br>
#@Date: 2/21/19<br>
#@GitHub Repository: https://github.com/alangarbarino/ETL_Project


# EXTRACT: SCRAPING iwaspoisoned.com
#* This Python code performs scraping of the iwaspoisoned.com website, capturing incident information on each page and across multiple pages
#* The information scraped in inserted into a MongoDB database `etl_db` in the collection `iwp` using the dictionary/document format provided below.
#* The code is structure as a set of functions:
#    * scrape_iwp(a_startpage=1, a_pagecount=20000): The function to be called by external programs to perform scraping and provide results in MongoDB.  The arguments are optional and the defaults values are as indicated.
#    
#    * Supporting functions:
#        * parse_incident_page(): This function is called for each iwaspoisoned.com website.  I uses the functions parse_one_incident() to capture the required information for each incident and returns an array of dictionaries.
#        * parse_one_incident(): This function is called for each incident and captures needed per-incident data, including a call to get_incident_details() to scrape incident address information from the per-incident detail page.
#        * get_incident_detail(): This function is called on a per-incident basis to navigate to the incident detail page to collect information not available from the main page, specifically: address.
        

# HOW THE `iwp` COLLECTION WAS EXPORTED
#The `iwp` collection in the `etl_db` database was exported to the file `mongodb_export_iwp.json` using:<br>
#```mongoexport --db etl_db --collection iwp --out mongodb_export_iwp.json```
#
#Later, `iwp` was filtered for only Illinois incidents, which were then stored in another collection `iwp_illinois_only`.  This collection was exported using:<br>
#```mongoexport --db etl_db --collection iwp_illinois_only --out mongodb_export_iwp.json```


# HOW THE `iwp` COLLECTION CAN BE IMPORTED 
#The JSON can be imported to the `iwp` collection in the `etl_db` database from file `mongodb_export_iwp.json` using:<br>
#```mongoimport --db etl_db --collection iwp --file mongodb_export_iwp.json```


# DOCUMENT FORMAT
#The documents in the etl_db collection iwp have the following formats:
#1. United States - Address is parsed
#<br>
#
#```{'_id': ObjectId('5c6a8db6aaa45be6f85938c7'),
# 'incident_title': "BJ's Restaurant & Brewhouse, Red Bug Lake Road, Oviedo, FL, USA",
# 'incident_date': 'Dec 30 2018 10:51pm',
# 'incident_url': 'https://iwaspoisoned.com/incident/bjs-restaurant-brewhouse-red-bug-lake-road-oviedo-fl-usa-155865#emailscroll',
# 'incident_report_type': 'Food Poisoning',
# 'incident_symptoms': ['Diarrhea', 'Nausea', 'Vomiting'],
# 'incident_misc': '',
# 'incident_address_info':
#     {'incident_address': '8285 Red Bug Lake Road, Oviedo, 32765 Florida, United States',
#      'incident_address_standard': '8285 Red Bug Lake Road, Oviedo, Florida 32765, United States',
#      'incident_address_street': '8285 Red Bug Lake Road',
#      'incident_address_street2': '',
#      'incident_address_street3': '',
#      'incident_address_city': 'Oviedo',
#      'incident_address_state': 'Florida',
#      'incident_address_zipcode': '32765',
#      'incident_address_country': 'United States'},
# 'incident_description': 'Ate there Friday evening, got sick about 8 hours later'}```
#
#<br>


#2. Other Countries - Address is not parsed
#(Should only USA entries in the database due to filtering, but just in case...)
#<br>
#
#```{'_id': ObjectId('5c6a8db6aaa45be6f85938c7'),
# 'incident_title': "BJ's Restaurant & Brewhouse, Red Bug Lake Road, Oviedo, FL, USA",
# 'incident_date': 'Dec 30 2018 10:51pm',
# 'incident_url': 'https://iwaspoisoned.com/incident/bjs-restaurant-brewhouse-red-bug-lake-road-oviedo-fl-usa-155865#emailscroll',
# 'incident_report_type': 'Food Poisoning',
# 'incident_symptoms': ['Diarrhea', 'Nausea', 'Vomiting'],
# 'incident_misc': '',
# 'incident_address_info':
#     {'incident_address': '8285 Red Bug Lake Road, Oviedo, 32765 Florida, United States'},
# 'incident_description': 'Ate there Friday evening, got sick about 8 hours later'}```
#
#<br>

# Extract Data from iwaspoisoned.com website using web scraping.
# Then populate the information in a MongoDB
# (to facilitate teaming, export the MongoDB to a JSON file)

# Dependencies
import pandas as pd
from sqlalchemy import create_engine
from pprint import pprint

from splinter import Browser
from bs4 import BeautifulSoup
import requests
import pymongo

import time

# Support export to and import from JSON file
import json

# ************************************************************************************
# FUNCTION: get_incident_detail()
# This function accepts a url that points to a single incident detail page
# and returns a dictionary with info from that page.
#
# Note: This function performs special parsing of addresses in the United States.
# Addresses in the US will be parsed down to individual components
# (street, street2, street3, city, state, zipcode, country)
# In addition, these parsed components are then recombined
# to form the full address in "standard" format
# (i.e., Zipcode after the state instead of before state)
#
# Addresses for other countries are provided only as an address string
#
# Arguments:
#    incident_detail_url: URL of the incident detail page
#
# Returns:
#    retval: A dictionary containing the incident detail page info

def get_incident_detail(a_url):

    # URL of page to be scraped
    # url_incident = 'https://iwaspoisoned.com/incident/chick-fil-a-north-fairfield-road-beavercreek-oh-usa-168576#emailscroll'
    # url_incident = 'https://iwaspoisoned.com/incident/subway-terminal-3-silver-dart-drive-toronto-on-canada-168642#emailscroll'
    if len(a_url) == 0:
        return None
    
    url_incident = a_url
    
    # Retrieve page with the requests module
    response = requests.get(url_incident)

    # Create BeautifulSoup object; parse with 'lxml'
    soup = BeautifulSoup(response.text, 'lxml')

    # Examine the results, then determine element that contains sought info
    # results are returned as an iterable list
    results = soup.find_all('div', class_='single-incident')

    for r in results:
        # Incident detail page - title
        # incident_detail_title = r.find('h1', class_='h1 post-title').text.strip()

        # Address
        try:
            addr_info = r.find('span', class_='pl-1 py-0 text-muted').text.strip()
            incident_address = ' '.join(addr_info.split())
            
        except AttributeError:
            addr_info = ""
            incident_address = ""

        # Ok, we now have an address of the form:
        # 2360 North Fairfield Road, Beavercreek, 45431 Ohio, United States
        # But, would be nice to be able to break this up into
        # individual components to facilitate address matching,
        # Especially with the non-standard location of the zipcode
        if "United States" in incident_address:
            # Create a list of address items
            ai_list = incident_address.split(',')

            # Some items are mandatory and are at the end of the list of length = N
            # N-1: Country e.g. "United States"
            # N-2: Zipcode and State e.g. "45431 Ohio"
            # N-3: City
            # Other entries 0 to N-4: Street/Apt/etc.

            # Get the count of how many components are in the address
            ai_size = len( ai_list )
            
            # Country
            try:
                incident_address_country = ai_list[ai_size-1].strip()
            except IndexError:
                incident_address_country = ""

            # Split the next entry to get state and zipcode
            try:
                zs_info = ai_list[ai_size-2].strip()
                zs_delim = zs_info.find(' ')
                # print(f"zs_delim: {zs_delim}, zs_info: {zs_info}")
                incident_address_zipcode = zs_info[:zs_delim].strip()
                incident_address_state = zs_info[zs_delim:].strip()

            except IndexError:
                incident_address_zipcode = ""
                incident_address_state = ""
                
            # City
            try:
                incident_address_city = ai_list[ai_size-3].strip()
                
            except IndexError:
                incident_address_city = ""

            # Process up to 3 "street" type entries
            incident_address_street = ""
            incident_address_street2 = ""
            incident_address_street3 = ""

            # print(f"ai_size: {ai_size}")
            # First street address item
            if ai_size >= 4:
                try:
                    incident_address_street = ai_list[0].strip()
                except:
                    incident_address_street = ""

            # Second street address item
            if ai_size >= 5:
                try:
                    incident_address_street2 = ai_list[1].strip()

                except:
                    incident_address_street2 = ""

            # Third street address item
            if ai_size >= 6:
                try:
                    incident_address_street3 = ai_list[i].strip()
                
                except:
                    incident_address_street3 = ""

            # Rebuild the address - with standard state then zipcode formating
            incident_address_standard = incident_address_street
            
            if len(incident_address_street2) > 0:
                incident_address_standard += ", " + incident_address_street2
            if len(incident_address_street3) > 0:
                incident_address_standard += ", " + incident_address_street3
            if len(incident_address_city) > 0:
                incident_address_standard += ", " + incident_address_city
            if len(incident_address_state) > 0:
                incident_address_standard += ", " + incident_address_state
            if len(incident_address_zipcode) > 0:
                incident_address_standard += " " + incident_address_zipcode
            if len(incident_address_country) > 0:
                incident_address_standard += ", " + incident_address_country


            #print(f">>> Incident Detail - Address: {incident_address}")
            #print(f">>> Incident Detail - Address - Standard: {incident_address_standard}")
            #print(f">>> Incident Detail - Address - Street: {incident_address_street}")
            #print(f">>> Incident Detail - Address - Street2: {incident_address_street2}")
            #print(f">>> Incident Detail - Address - Street3: {incident_address_street3}")
            #print(f">>> Incident Detail - Address - City: {incident_address_city}")
            #print(f">>> Incident Detail - Address - State: {incident_address_state}")
            #print(f">>> Incident Detail - Address - Zipcode: {incident_address_zipcode}")
            #print(f">>> Incident Detail - Address - Country: {incident_address_country}")
            #print("-"*40)

            # Place all this good info into a dictionary
            detail_post_item = {
                'incident_address': incident_address,
                'incident_address_standard': incident_address_standard,
                'incident_address_street': incident_address_street,
                'incident_address_street2': incident_address_street2,
                'incident_address_street3': incident_address_street3,
                'incident_address_city': incident_address_city,
                'incident_address_state': incident_address_state,
                'incident_address_zipcode': incident_address_zipcode,
                'incident_address_country': incident_address_country
            }

        else:
            # Ok, for other countries, don't try to parse the incident_address
            # print(f">>> Incident Detail - Address: {incident_address}")
            # print("-"*40)

            # Place all this good info into a dictionary
            detail_post_item = {
                'incident_address': incident_address,
            }

        # pprint(detail_post_item)
        
        return detail_post_item
    


# ************************************************************************************
# FUNCTION: parse_one_incident()
# This function accepts a Beautiful Soup object that contains a single incident
# and returns a dictionary with info for that incident.
# This includes a call to the get_incident_detail() function,
# which gets needed information from the detail page for this incident
#
# Arguments:
#    a_bsobj: A Beautiful Soup object containing a single incident
#
# Returns:
#    retval: A dictionary containing the incident detail page info

def parse_one_incident(a_bsobj):
    
    # Create BeautifulSoup object; parse with 'lxml'
    r = a_bsobj

    # Get the primary incident report info from the main box
    main_box = r.find('div', class_='report-first-box')
    
    # Date the incident occurred
    try:
        incident_date = main_box.find('p', class_ = 'report-date').text.strip()
        
    except AttributeError:
        incident_date = ""
        

    # Title of the incident
    try:
        incident_title = main_box.find('a')['title']

    except AttributeError:
        incident_title = ""
    
    # Remove the tag phrase from the title if it's present
    if "- Got Food Poisoning? Report it now" in incident_title:
        i_delim = incident_title.find("- Got Food Poisoning? Report it now")
        incident_title = incident_title[:i_delim].strip()

    # URL of the per-incident details
    try:
        incident_url = main_box.find('a')['href'].strip()

    except AttributeError:
        incident_url = ""

    # Get the Symptoms
    report_tags = main_box.find_all('p', class_ = 'report-tag')

    # Parse each report tag into its proper field
    incident_symptoms = ""
    incident_report_type = ""
    incident_misc = ""

    for rt in report_tags:
        # Get the text in this tag
        rt_info = rt.text.strip()

        # Symptoms
        if "Symptoms:" in rt_info:
            incident_symptoms = [ s.replace(',','') for s in rt_info[len("Symptoms: "):].split() ]

        # Report Type
        elif "Report Type:" in rt_info:
            incident_report_type = rt_info[len("Report Type: "):]

        # Ok... no idea what this report tag contains
        else:
            incident_misc = rt_info

    #pprint(main_box)
    #print(f">>> Incident Date: {incident_date}")
    #print(f">>> Incident Title: {incident_title}")
    #print(f">>> Incident URL: {incident_url}")
    #print(f">>> Incident Report Type: {incident_report_type}")
    #print(f">>> Incident Symptoms: {incident_symptoms}")
    #print(f">>> Incident Misc Info: {incident_misc}")
    #print("-"*40)

    # Get the full description of the incident
    # Assume this couple be populated in multiple paragraphs
    desc_box = r.find('div', class_='report-second-box')
    desc_list = desc_box.find_all('p')
    incident_description = ""
    for d in desc_list:
        incident_description += d.text.strip()

    #pprint(descbox)
    #print(f">>> Description: {incident_description}")
    #print("-"*40)

    # Go to the detail page to get the one piece of info we
    # need that's not on the main page - the address!
    incident_address_info = get_incident_detail(incident_url)

    # Place all this good info into a dictionary
    post_item = {
        'incident_title': incident_title,
        'incident_date': incident_date,
        'incident_url': incident_url,
        'incident_report_type': incident_report_type,
        'incident_symptoms': incident_symptoms,
        'incident_misc': incident_misc,
        'incident_address_info': incident_address_info,
        'incident_description': incident_description
    }
    #pprint(post_item)

    return post_item


# ************************************************************************************
# FUNCTION: parse_incident_page()
# This function accepts an HTML string from an
# IWP website page that contains multiple incidents.
# It then loops through the incidents on the page and the uses parse_one_incident()
# function to grab the relevant incident info from the page.
#
# NOTE: The incidents are filtered to keep only those that occurred in the USA
# since our project is focused on Chicago, IL.
#
# Arguments:
#    a_html: A string of HTML content containing multiple incidents
#
# Returns:
#    retval: A list of dictionaries of USA incident information

def parse_incident_page(a_html):
    
    # Do a basic check
    if len(a_html) == 0:
        return None

    # Create BeautifulSoup object; parse with 'lxml'
    soup = BeautifulSoup(a_html, 'lxml')

    # Examine the results, then determine element that contains sought info
    # results are returned as an iterable list
    results = soup.find_all('div', class_='row div-report-box')

    # Keep track of how many entries we've added
    n_incidents = 0

    # Get info for all of the incidents on this page
    incident_list = []
    try:
        for r in results:

            # Parse this incident
            incident_info = parse_one_incident(r)
            #pprint(incident_info)

            # Only retain incidents in the United States
            # (Our scope is City of Chicago, so keeping all of USA should be sufficient)
            if "United States" in incident_info['incident_address_info']['incident_address']:
                
                # Only retain incidents in Illinois
                if "Illinois" in incident_info['incident_address_info']['incident_address']:

                    # Append this Illinois, USA incident to the list
                    incident_list.append( incident_info )
                    n_incidents += 1

                    # Print a progress message
                    # print(f">> Added incident #{n_incidents}: {incident_info['incident_title']}")

            #DEBUG ****************************************
            #if n_incidents > 3:
            #    break

    except TypeError:
        # If an iterable is not provided in "results", then fail gracefully
        pass
            
            
    # Return the list of dictionaries with USA incident info
    return incident_list



# ************************************************************************************
# FUNCTION: scrape_iwp()
# This function accepts an HTML string from an
# IWP website page that contains multiple incidents.
# It then loops through the incidents on the page and the uses parse_one_incident()
# function to grab the relevant incident info from the page.
#
# NOTE: The incidents are filtered to keep only those that occurred in the USA
# since our project is focused on Chicago, IL.
#
# Arguments:
#    a_startpage: Optional argument specifying the website page at which to start scraping
#    a_pagecount: Optional argument specifying how many pages to scrape
#
# Returns:
#    retval: A list of dictionaries of USA incident information

def scrape_iwp(a_startpage=1, a_pagecount=20000):

    # Initialize PyMongo to work with MongoDBs
    conn = 'mongodb://localhost:27017'
    client = pymongo.MongoClient(conn)

    # Define database and collection
    db = client.etl_db

    # Setup the splinter Browser
    executable_path = {'executable_path': 'chromedriver.exe'}
    browser = Browser('chrome', **executable_path, headless=False)

    # URL of page to be scraped
    # url_iwp = 'https://iwaspoisoned.com'
    # UPDATE: Added the "?page=" to restart scraping on pages not already obtained
    
    url_iwp = 'https://iwaspoisoned.com/?page=' + str(a_startpage)

    # Visit the IWP page
    browser.visit( url_iwp )

    # Extract incidents from multiple pages
    page_target = int(a_pagecount)

    # How long to wait between pages to avoid triggering issues on website
    page_wait = 2

    # Count the number of pages visited
    n_pages = 0

    # Loop until no more pages or until page target is reached
    full_incident_list = []
    for j in range(page_target):
        # Get a page full of incidents from the USA
        i_list = parse_incident_page(browser.html)
        n_pages += 1

        # Add this list of incidents to a running list
        # full_incident_list.extend(i_list)

        # Add this list of incidents to the Mongo database
        
        try:
            # Attempt the insert
            insert_results = db.iwp.insert_many(i_list)
            
            # Print a progress marker
            print(f"Page {n_pages} of {a_pagecount}: {len(insert_results.inserted_ids)} of {len(i_list)} incidents added to DB. Total incidents: {db.iwp.count_documents({})}")

        except TypeError:
            # It's possible the incident list was empty, which could trigger a TypeError.
            # This is the case since it is being filtered for only Illinois, USA incidents
            print(f">> Page {n_pages}: No incidents captured")
        
        # Check to see if a hyperlink with attribute 'rel' = 'next' is present
        soup_thispage = BeautifulSoup(browser.html, 'lxml')
        next_tag = soup_thispage.find('a', {'rel' : 'next'})

        if next_tag:
            # Ok, there is a next page - get the hyperlink
            # print(f"DEBUG: Going to next page (next_tag = '{next_tag}' ")
            try:
                next_page_url = next_tag['href']

                # Wait for a specified number of seconds
                time.sleep(page_wait)

                # Click it!
                browser.click_link_by_href(next_page_url)

                #DEBUG ****************************************
                # if n_pages > 3:
                #    break

            # If KeyError occurs, then this tag has no html link for some reason
            except KeyError:
                break

        else:
            # No more pages - break out of this loop
            break
    
    # Close the Browser
    browser.quit()
            
    # Return the number of pages scraped
    return n_pages


# EXAMPLE:
# Command to Start at Page 1 of iwaspoisoned.com and Scrape 10 Pages,
# only keeping Incidents that occurred in Illinois, USA
#
# In a _separate_ Python file, include the code below:

#*******************************************************************************
# Import ETL Scraper function `scrape_iwp` from the local file `etl_scrape_iwp`
# from etl_scrape_iwp import scrape_iwp
#
# Use the function to scape pages
# pages_scraped = scrape_iwp(1, 10)
#*******************************************************************************