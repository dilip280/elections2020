# -*- coding: utf-8 -*-
"""
Created on Tue Nov  3 09:01:21 2020

@author: dilip
"""        
import glob
import os
import pandas as pd
import numpy as np     
'''
--------------------------------------------------------------------------------------------- 
Create dataframe of poll data from 270toWin
--------------------------------------------------------------------------------------------- 
'''
def parse_electoral_files_of_all_states(output_path,file_path):

    poll_df = pd.DataFrame(columns=['State','Poll Source','New_Source','Poll Date','Sample Size','Reported proportion for Biden','Reported proportion for Trump','Other'])
    dir = os.path.join(output_path, '*.csv')

    for file in glob.glob(dir):
            replace_string = file_path + os.sep + 'PollsByState' + os.sep
            state = str(file).replace('.csv','').replace(output_path ,'')
            with open(file, 'r') as f:
                next(f)
                for l in f:
                    data = l.strip().split('\t')
                    data = [word for element in data for word in element.split(',')]
                    data.insert(0, state)
                    data.pop(1)
                    df_length = len(poll_df)
                    poll_df.loc[df_length] = data
                    poll_df['Poll Date'] = pd.to_datetime(poll_df['Poll Date'])

    return poll_df


'''
--------------------------------------------------------------------------------------------- 
Create dataframe of pollster data
--------------------------------------------------------------------------------------------- 
'''
def parse_pollster_data(file_path):
    pollster_df = pd.DataFrame(columns=['Pollster', 'Method', 'Polls Analyzed', 'Simple Avg Err',
                                        'Races Called Correctly', 'Advanced', 'Predictive', 'Poll Source Grade',
                                        'Party', 'Poll Source Bias'])
    dir = os.path.join(file_path, 'Pollster.txt')

    for file in glob.glob(dir):
        with open(file,'r', encoding='utf-8') as f:
            for l in f:
                pollster_data = l.strip().split('\t')
                if len(pollster_data) == 8:
                    pollster_data.append(None)
                    pollster_data.append(None)
                    df_length = len(pollster_df)
                    pollster_df.loc[df_length] = pollster_data
                else:
                    df_length = len(pollster_df)
                    pollster_df.loc[df_length] = pollster_data

    return pollster_df
                    
                
                
'''
--------------------------------------------------------------------------------------------- 
Create dataframe of the electoral votes for each state
--------------------------------------------------------------------------------------------- 
'''                
def parse_electoral_votes_data(file_path):
    electoralvotes_df = pd.DataFrame(columns=['US State', 'Electoral Votes'])
    dir = os.path.join(file_path, 'Electoral_Votes.txt')

    for file in glob.glob(dir):
        with open(file,'r', encoding='utf-8') as f:
            for l in f:
                electoralvotes_data = l.strip().split('\t')
                df_length = len(electoralvotes_df)
                electoralvotes_df.loc[df_length] = electoralvotes_data
    return electoralvotes_df  

'''
--------------------------------------------------------------------------------------------- 
Create dataframe for state abbreviations
--------------------------------------------------------------------------------------------- 
'''               
def parse_state_abbreviations(file_path):
    state_abbreviations_df = pd.read_csv(file_path + os.sep + 'StateAbbreviations.csv')
    return state_abbreviations_df


'''
--------------------------------------------------------------------------------------------- 
Enrich state codes in electoral votes dataset
--------------------------------------------------------------------------------------------- 
'''
def enrich_electoral_votes_with_state_codes(electoralvotes_df, state_abbreviations_df):
    final_df = electoralvotes_df.merge(state_abbreviations_df[['State','Abbrev','Code']], how='left',
                             left_on=['US State'], right_on=['State'])
    
    for index, row in final_df.iterrows():
        state = row['US State']
        state = state.lower().replace(' ', '-')
        row['US State'] = state
    del final_df['State']
    return final_df

'''
--------------------------------------------------------------------------------------------- 
Create final dataframe
--------------------------------------------------------------------------------------------- 
'''
def merge_electoral_votes_with_pollster_data(poll_df, pollster_df):
                
    final_df = poll_df.merge(pollster_df[['Pollster','Poll Source Grade','Party','Poll Source Bias']], how='left', 
                             left_on=['New_Source'], right_on=['Pollster'])
    final_df['Reported proportion for Biden'] = final_df['Reported proportion for Biden'].str.rstrip('%').astype('float') / 100
    final_df['Reported proportion for Trump'] = final_df['Reported proportion for Trump'].str.rstrip('%').astype('float') / 100
    final_df['Other'] = final_df['Other'].str.rstrip('%').astype('float') / 100
    final_df['Sample Size'] = final_df['Sample Size'].replace(',','', regex=True).replace('N/A',0).astype('int')

    final_df['Estimated_Biden'] = final_df['Reported proportion for Biden'] + (final_df['Other'] / 2)
    final_df['Estimated_StdDev'] = np.sqrt( (final_df['Estimated_Biden'] * (1 - final_df['Estimated_Biden'])) / 
                                           final_df['Sample Size'] )

    del final_df['Other']
    del final_df['Pollster']
    return final_df

    
'''
--------------------------------------------------------------------------------------------- 
Main program logic
--------------------------------------------------------------------------------------------- 
'''
file_path = os.getcwd()
output_path = file_path+os.sep+'PollsByState'+os.sep
poll_df = parse_electoral_files_of_all_states(output_path, file_path)
poll_df.to_csv(file_path + os.sep + 'poll_df.csv')
pollster_df = parse_pollster_data(file_path)
pollster_df.to_csv(file_path + os.sep + 'pollster_df.csv')
final_df = merge_electoral_votes_with_pollster_data(poll_df, pollster_df)
final_df.to_csv(file_path + os.sep + 'final_df.csv')
electoralvotes_df = parse_electoral_votes_data(file_path)

state_abbreviations_df = parse_state_abbreviations(file_path)
state_abbreviations_df.to_csv(file_path + os.sep + 'state_abbreviations_df.csv')
electoralvotes_df = enrich_electoral_votes_with_state_codes(electoralvotes_df, state_abbreviations_df)
electoralvotes_df.to_csv(file_path + os.sep + 'electoralvotes_df.csv')
print ("Created dataframes and exported to csv..")  