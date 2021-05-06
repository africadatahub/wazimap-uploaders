import sys
import requests
import pandas as pd
import numpy as np

ENDPOINT = sys.argv[1]
TOKEN = sys.argv[2]
OWID_FILE_URL = "https://github.com/owid/covid-19-data/raw/master/public/data/owid-covid-data.csv"
OWID_FILE_PATH = "/tmp/owid-covid-data.csv"
WAZIMAP_FILE_PATH = "/tmp/owid_Vaccinations_DosesReceived.csv"
DATASET_ID = 953


USE_COLS_POPULATION = ['iso_code', 'continent', 'date', 'stringency_index', 'population', 'population_density', 'median_age', 'aged_65_older', 'aged_70_older', 'gdp_per_capita',
                       'extreme_poverty', 'cardiovasc_death_rate', 'diabetes_prevalence', 'female_smokers', 'male_smokers', 'handwashing_facilities', 'hospital_beds_per_thousand',
                       'life_expectancy', 'human_development_index', 'new_tests', 'total_tests', 'positive_rate', 'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated',
                      'new_vaccinations', 'new_vaccinations_smoothed', 'total_vaccinations_per_hundred', 'people_vaccinated_per_hundred', 'people_fully_vaccinated_per_hundred',
                      'total_tests_per_thousand', 'new_tests_per_thousand', 'new_tests_smoothed_per_thousand', 'total_cases_per_million', 'total_deaths_per_million', 'positive_rate']


## Fetch the OWID file
owid_r = requests.get(OWID_FILE_URL, allow_redirects=True)
owid_r.raise_for_status()
open(OWID_FILE_PATH, 'wb').write(owid_r.content)



# Load the Citizen CSV as a pandas dataframe, but only selected columns
owid = pd.read_csv(OWID_FILE_PATH, delimiter=",", usecols=USE_COLS_POPULATION)

owid['date'] =  pd.to_datetime(owid['date'], format='%Y-%m-%d')
owid = owid.rename(columns={'iso_code': 'Geography', 'date': 'Date'})

owid = owid.replace(to_replace ="OWID_AFR",
                 value ="Africa")
owid = owid.replace(to_replace ="OWID_ASI",
                 value ="Asia")
owid = owid.replace(to_replace ="OWID_EUR",
                 value ="Europe")
owid = owid.replace(to_replace ="OWID_NAM",
                 value ="North America")
owid = owid.replace(to_replace ="OWID_SAM",
                 value ="South America")
owid = owid.replace(to_replace ="OWID_OCE",
                 value ="Oceania")




# Initial transformation and extraction of vaccinations administered
vac1 = owid[["Geography", "Date", "people_vaccinated"]]
vac1 = vac1[vac1["Date"] > "2021-02-01"]

vac1 = vac1.pivot(index='Geography', columns='Date')
vac1.columns = vac1.columns.droplevel(0)
vac1['LastValue'] = vac1.iloc[:, 1:].ffill(axis=1).iloc[:, -1]
vac1.drop(vac1.columns.difference(['Geography','LastValue']), 1, inplace=True)
vac1['Indicator'] = "At least one vaccine dose"
vac1.reset_index(level=0, inplace=True)
vac1 = vac1.astype(object).replace(np.nan, 'Null')
vac1 = vac1.rename(columns={'LastValue': 'Count'})

vac2 = owid[["Geography", "Date", "people_fully_vaccinated"]]
vac2 = vac2[vac2["Date"] > "2021-02-01"]

vac2 = vac2.pivot(index='Geography', columns='Date')
vac2.columns = vac2.columns.droplevel(0)
vac2['LastValue'] = vac2.iloc[:, 1:].ffill(axis=1).iloc[:, -1]
vac2.drop(vac2.columns.difference(['Geography','LastValue']), 1, inplace=True)
vac2['Indicator'] = "All doses prescribed by the vaccination protocol"
vac2.reset_index(level=0, inplace=True)
vac2 = vac2.astype(object).replace(np.nan, 'Null')
vac2 = vac2.rename(columns={'LastValue': 'Count'})

tmp = [vac1, vac2]
vaccinations_totals = pd.concat(tmp)

vaccinations_totals = vaccinations_totals[vaccinations_totals['Count'].notna()]
vaccinations_totals.to_csv(WAZIMAP_FILE_PATH, index = False, sep=',')




## Upload to wazimap
url = f"{ENDPOINT}/api/v1/datasets/{DATASET_ID}/upload/"

headers = {'authorization': f"Token {TOKEN}"}
files = {'file': open(WAZIMAP_FILE_PATH, 'rb')}
payload = {'update': True, 'overwrite': True}

wazi_r = requests.post(url, headers=headers, data=payload, files=files)
wazi_r.raise_for_status()

print(wazi_r.json())
