# -*- coding: utf-8 -*-
"""
Created on Tue Nov  3 05:39:00 2020

@author: dilip
"""
import glob
import os
import requests
from bs4 import BeautifulSoup
from pollster_name_changes import pollster_changes
import pandas as pd
'''
--------------------------------------------------------------------------------------------- 
Cleans the output directory
--------------------------------------------------------------------------------------------- 
'''
def clear_directory(output_path):
    files = glob.glob(output_path+'*')
    for f in files:
        os.remove(f)
      
'''
--------------------------------------------------------------------------------------------- 
Generates the files by parsing the polls data from 270towin.com 
--------------------------------------------------------------------------------------------- 
'''    
def generate_polls_by_state(output_path,State_List):

    poll_sources = []
    replaced_poll_sources = []
    poll_dates = []
    poll_samples = []
    democratic_candidate = []
    republican_candidate = []

    for current_state in State_List:
        state = current_state + '/'
        state = state.lower().replace(' ', '-')

        poll_sources.clear()
        replaced_poll_sources.clear()
        poll_dates.clear()
        poll_samples.clear()
        democratic_candidate.clear()
        republican_candidate.clear()

        URL = 'https://www.270towin.com/2020-polls-biden-trump/' + state
        page = requests.get(URL)
        soup = BeautifulSoup(page.content, 'html.parser')
        results = soup.find('div', class_="polls-wrapper")

        polls = results.find_all('td', class_='poll_src')
        for element in polls:
            value = element.text
            poll_sources.append(value.replace('\n', ''))

        poll_date = results.find_all('td', class_='poll_date')
        for dates in poll_date:
            poll_dates.append(dates.text)

        sample = results.find_all('td', class_='poll_sample')
        for poll_sample in sample:
            poll_samples.append(
                poll_sample.text.replace('\n', '').replace('\t', '').replace('A', '').replace('RV', '').replace('LV', '').replace(' ',
                                                                                                                 '').replace(
                    ',', '').replace('N/','0').replace('N/A','0').split('&plusmn')[0])


        not_winner = results.find_all('td', class_='poll_data')
        for candidate in not_winner:
            if candidate['candidate_id'] == 'D':
                democratic_candidate.append(candidate.text.replace(' ', '').replace('\n', '').replace('%',''))
            else:
                republican_candidate.append(candidate.text.replace(' ', '').replace('\n', '').replace('%',''))

        for new_poll_name in poll_sources:
            for pollster_name, changed_name in pollster_changes.items():
                if pollster_name == new_poll_name:
                    new_poll_name = changed_name
            replaced_poll_sources.append(new_poll_name)

        frame = pd.DataFrame(poll_sources)
        frame.columns = ['Source']
        frame['New_Source'] = replaced_poll_sources
        frame['Date'] = poll_dates
        frame['Sample'] = poll_samples
        frame['Biden'] = democratic_candidate
        frame['Trump'] = republican_candidate
        frame['Other'] = 100 -frame['Trump'].astype('float') - frame['Biden'].astype('float')

        frame.to_csv(output_path + state.replace('/', '') + '.csv')
        frame.dropna(axis=0, how='all')
        
        
'''
--------------------------------------------------------------------------------------------- 
Main program logic
--------------------------------------------------------------------------------------------- 
'''
file_path = os.getcwd()
output_path = file_path+os.sep+'PollsByState'+os.sep
USStates = file_path+os.sep+'StateAbbreviations.csv'

clear_directory(output_path)
if not os.path.exists(output_path):
    os.makedirs(output_path)
df = pd.read_csv(USStates, index_col='State')
state_list = df.index.values
generate_polls_by_state(output_path,state_list) 
print ("Data Files generated after parsing the content..")       