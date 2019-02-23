#This function accesses the data from the City of Chicago API 
# and transforms it into a Pandas DataFrame. 
# Once it is in Pandas, the funcition will formatt the all data types to objects 
# and will formatt the time into 'yyyy-mm-dd'. 
# It also deals with missing data. 
# The result will be a cleaned up formatted pandas dataframe 
# ready to be transformed into SQLite.


def api_to_pandas():
    # URL to acces the latest 150K records
    url = 'https://data.cityofchicago.org/resource/cwig-ma7x.json?$limit=150000'
    response = requests.get(url).json()
    # Transform into Pandas
    df = pd.DataFrame(response)
    #Select useful columns
    df = df[['dba_name', 'license_', 'facility_type',
       'risk', 'address', 'zip', 'inspection_date',
       'inspection_type', 'results', 'latitude', 'longitude']]
    # Rename columns to facilitate merging
    df.rename(columns={'dba_name': 'business_name', 'license_': 'license'}, inplace=True)
    # Drop missing data
    inspections_complete = df.dropna()
    # Formatt Time columns
    split = inspections_complete['inspection_date'].str.split('T').str[0]
    inspections_complete['date'] = split
    del inspections_complete['inspection_date']
    inspections_complete.rename(columns={'date': 'inspection_date'}, inplace=True)
    # From Float to Integer (to get rid of the decimal) and then to String.
    inspections_complete['license'] = inspections_complete['license'].astype(int)
    inspections_complete['license'] = inspections_complete['license'].astype(str)

    # From Float to Integer (to get rid of the decimal) and then to String.
    inspections_complete['zip'] = inspections_complete['zip'].astype(int)
    inspections_complete['zip'] = inspections_complete['zip'].astype(str)

    # From Float to String
    inspections_complete['latitude'] = inspections_complete['latitude'].astype(str)
    inspections_complete['longitude'] = inspections_complete['longitude'].astype(str)
    
    return inspections_complete