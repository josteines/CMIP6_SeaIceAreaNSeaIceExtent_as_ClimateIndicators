import xarray as xr
import numpy as np 
import pandas as pd
import dask
import os
from QueryESGF4RequiredVariables import query_and_open_OPeNDAP_urls_from_ESGF
from QueryESGF4EnsambleMembers import query_ESGF_nodes_4_ensable_members
from utils import get_script_root_path, read_config_file, init_logging, calculateNprepare_siareanNsiextentn, alter_siarean_and_siextentn, siarean_and_siextentn2yearly_max_min

import logging
import sys

logger = init_logging()

# Get the root path to the current directory
root_path = get_script_root_path()

### read in the desired config file
config_file_path = f'{root_path}/config.yaml'
config = read_config_file(config_file_path)

models = config['models']
temporal_resolutions = config['temporal_resolutions']
scenarios = config['scenarios']
nodes = config['nodes']
indicator_output_path = config['indicator_output_path']
maxmin_indicator_output_path = config['maxmin_indicator_output_path']


# Initialize an empty dictionary
model_dictionary = {}

# Loop through the model names
for model in models:
    # Initialize a nested dictionary for each model
    model_dictionary[model] = {}
    
    # Loop through the temporal resolutions
    for resolution in temporal_resolutions:
        # Assign scenarios to each resolution
        model_dictionary[model][resolution] = scenarios

node_urls = [f'https://{node}/esg-search' for node in nodes]


# Iterate through the complete dictionary
for model, frequencies in model_dictionary.items():
    print(f"Model: {model}")
    for Frequency, scenarios in frequencies.items():
        print(Frequency, scenarios)
        if Frequency == 'Daily':
            filename_spesification = 'SIday'
            frequency = 'day'
        elif Frequency == "Monthly":
            filename_spesification = 'SImon'
            frequency = 'mon'
        print(f"  Frequency: {Frequency}, frequency: {frequency}")

        for scenario in scenarios:
            print(f"    Scenario: {scenario}")

            if (model == 'NorESM2-LM' and scenario == 'ssp460') or (model == 'ACCESS-CM2' and scenario == 'ssp460'):  # Skipping this scenario as the model do not have this scenario
                print('Skipping this scenario as the model do not have this scenario')
                continue

            node_availability = False
            out_of_nodes = False
            i = 0
            node_url = node_urls[0]
            while (node_availability == False) and (out_of_nodes == False):

                try:
                    ensamble_members = query_ESGF_nodes_4_ensable_members(node_url = node_url,
                                                                          project = 'CMIP6',
                                                                          experiment_id = scenario,
                                                                          variable_id = 'siconc',
                                                                          frequency = frequency,
                                                                          table_id = filename_spesification,
                                                                          source_id = model,
                    )

                    
                    if ensamble_members == None:
                        print(f'{node_url} returned empty query.', '\n')
                                
                    else:
                        node_availability = True
                        working_node_url = node_url

                except Exception as e:
                    print('\n', f'Could not extract available ensamble members from {node_url}: {e}', '\n')

                if node_url == node_urls[-1]:
                    out_of_nodes = True
                    print(f'out_of_nodes is set to {out_of_nodes}')
                else:
                    i += 1
                    node_url = node_urls[i]

            print('\n')
            if node_availability == True:
                node_url = working_node_url
                print(f'Managed to find the available ensamble members on the data node {node_url}:')
                print(ensamble_members)
            else:
                print(f'Did not manage to find the available ensamble members on either of the following data nodes: {nodes}')
                sys.exit()


            for ensamble_member in ensamble_members:
                if model == 'EC-Earth3-Veg' and ('r6' in ensamble_member or 'r5' in ensamble_member): # This model lacks areacello for the r5 and r6 cases
                     continue
                 
                print(f"      Ensamble member: {ensamble_member}")

            
                if (model == 'NorESM2-LM' and Frequency == 'Monthly') or (model == 'ACCESS-CM2' and Frequency == 'Monthly') or (model == 'MRI-ESM2-0' and Frequency == 'Monthly'):
                    print('Already made siarean and siextentn cases!')
                    print('Monthly resolution are made')

                    
                    node_availability = False
                    out_of_nodes = False
                    i = 0
                    node_url = node_urls[0]
                    while (node_availability == False) and (out_of_nodes == False):
                    
                        try:
                            siarean_ds = query_and_open_OPeNDAP_urls_from_ESGF(node_url = node_url,
                                                                                    project = 'CMIP6',
                                                                                    experiment_id = scenario,
                                                                                    variable_id = 'siarean',
                                                                                    variant_label = ensamble_member,
                                                                                    frequency = frequency,
                                                                                    table_id = filename_spesification,
                                                                                    source_id = model,
                                                                                    )
                            
                            siextentn_ds = query_and_open_OPeNDAP_urls_from_ESGF(node_url = node_url,
                                                                        project = 'CMIP6',
                                                                        experiment_id = scenario,
                                                                        variable_id = 'siextentn',
                                                                        variant_label = ensamble_member,
                                                                        frequency = frequency,
                                                                        table_id = filename_spesification,
                                                                        source_id = model,
                                                                        )

                            if not isinstance(siarean_ds, xr.Dataset) or not isinstance(siextentn_ds, xr.Dataset):
                                print(f'{node_url} did not return the siarean and siextentn datasets')
                            
                            else:
                                node_availability = True
                                working_node_url = node_url

                        # except UnboundLocalError: # If opendap_url won't open
                        except Exception as e:
                            print('\n', f'Could not extract available ensamble members from {node_url}: {e}', '\n')

                        if node_url == node_urls[-1]:
                            out_of_nodes = True
                            print(f'out_of_nodes is set to {out_of_nodes}')
                        else:
                            i += 1
                            node_url = node_urls[i]


                    print('\n')
                    if node_availability == True:
                        node_url = working_node_url
                        print(f'Managed to find the opendap_url(s) and open the dataset with the data node: {node_url}')
                    else:
                         print(f'Did not manage to find the opendap urls within either of the following data nodes: {nodes}')
                         sys.exit()
                    
                    
                    # Creating the paths if they do not already exist
                    siarean_path = indicator_output_path+'/'+f'siarean/{model}_sea_ice/{Frequency}/{scenario}/{ensamble_member}/'
                    siextentn_path = indicator_output_path+'/'+f'siextentn/{model}_sea_ice/{Frequency}/{scenario}/{ensamble_member}/'


                    # Ensure the directory structure exists
                    siarean_directory = os.path.dirname(siarean_path)
                    if not os.path.exists(siarean_directory):
                        os.makedirs(siarean_directory)  # Create all missing directories

                    siextentn_directory = os.path.dirname(siextentn_path)
                    if not os.path.exists(siextentn_directory):
                        os.makedirs(siextentn_directory)  # Create all missing directories

                    siarean_dataset, siextentn_dataset = alter_siarean_and_siextentn(siarean_dataset = siarean_ds,
                                                                    siextentn_dataset = siextentn_ds,
                                                                    siarean_path = siarean_path+f'/siarean_{filename_spesification}_{model}_{scenario}_{ensamble_member}_2015_2100.nc',
                                                                    siextentn_path = siextentn_path+f'/siextentn_{filename_spesification}_{model}_{scenario}_{ensamble_member}_2015_2100.nc')
                    
                    print('\n')
                    print(f'The siarean directory: {siarean_directory}')
                    print(f'The siextentn directory: {siextentn_directory}')
                    print('\n')

                    # Creating the paths if they do not already exist
                    siarean_maxmin_path = maxmin_indicator_output_path+'/'+f'siarean/{model}_sea_ice/{Frequency}/{scenario}/{ensamble_member}/'
                    siextentn_maxmin_path = maxmin_indicator_output_path+'/'+f'siextentn/{model}_sea_ice/{Frequency}/{scenario}/{ensamble_member}/'


                    # Ensure the directory structure exists
                    siarean_maxmin_directory = os.path.dirname(siarean_maxmin_path)
                    if not os.path.exists(siarean_maxmin_directory):
                        os.makedirs(siarean_maxmin_directory)  # Create all missing directories

                    
                    # Ensure the directory structure exists
                    siextentn_maxmin_directory = os.path.dirname(siextentn_maxmin_path)
                    if not os.path.exists(siextentn_maxmin_directory):
                        os.makedirs(siextentn_maxmin_directory)  # Create all missing directories


                    print('Yearly resolution are made')
                    siarean_maxmin_dataset, siextentn_maxmin__dataset = siarean_and_siextentn2yearly_max_min(siarean_dataset = siarean_ds,
                                                                    siextentn_dataset = siextentn_ds,
                                                                    siarean_path = siarean_maxmin_path+f'siarean_{filename_spesification}_{model}_{scenario}_{ensamble_member}_2015_2100.nc',
                                                                    siextentn_path = siextentn_maxmin_path+f'siextentn_{filename_spesification}_{model}_{scenario}_{ensamble_member}_2015_2100.nc')
                    print('\n')
                    print(f'The siarean_max_min directory: {siarean_directory}')
                    print(f'The siextentn_max_min directory: {siextentn_directory}')
                    print('\n')

                else:
                    
                    # Creating the paths if they do not already exist
                    siarean_path = indicator_output_path+'/'+f'siarean/{model}_sea_ice/{Frequency}/{scenario}/{ensamble_member}/'
                    siextentn_path = indicator_output_path+'/'+f'siextentn/{model}_sea_ice/{Frequency}/{scenario}/{ensamble_member}/'
                    
                    # Ensure the directory structure exists
                    siarean_directory = os.path.dirname(siarean_path)
                    if not os.path.exists(siarean_directory):
                        os.makedirs(siarean_directory)  # Create all missing directories
                    else:
                         print(f'siarean_directory already exists: {siarean_directory}')

                    # Ensure the directory structure exists
                    siextentn_directory = os.path.dirname(siextentn_path)
                    if not os.path.exists(siextentn_directory):
                        os.makedirs(siextentn_directory)  # Create all missing directories
                    else:
                         print(f'siextentn_directory already exists: {siextentn_directory}')

                    # Creating the paths if they do not already exist
                    siarean_maxmin_path = maxmin_indicator_output_path+'/'+f'siarean/{model}_sea_ice/{Frequency}/{scenario}/{ensamble_member}/'
                    siextentn_maxmin_path = maxmin_indicator_output_path+'/'+f'siextentn/{model}_sea_ice/{Frequency}/{scenario}/{ensamble_member}/'

                    # Ensure the directory structure exists
                    siarean_maxmin_directory = os.path.dirname(siarean_maxmin_path)
                    if not os.path.exists(siarean_maxmin_directory):
                        os.makedirs(siarean_maxmin_directory)  # Create all missing directories
                    else:
                         print(f'siarean_maxmin_directory already exists: {siarean_maxmin_directory}')

                    
                    # Ensure the directory structure exists
                    siextentn_maxmin_directory = os.path.dirname(siextentn_maxmin_path)
                    if not os.path.exists(siextentn_maxmin_directory):
                        os.makedirs(siextentn_maxmin_directory)  # Create all missing directories
                    else:
                         print(f'siarean_maxsiextentn_maxmin_directoryin_directory already exists: {siextentn_maxmin_directory}')


                    node_availability = False
                    out_of_nodes = False
                    i = 0
                    node_url = node_urls[0]
                    while (node_availability == False) and (out_of_nodes == False):
                    
                        try:

                            ds = query_and_open_OPeNDAP_urls_from_ESGF(node_url = node_url,
                                                                                    project = 'CMIP6',
                                                                                    experiment_id = scenario,
                                                                                    variable_id = 'siconc',
                                                                                    variant_label = ensamble_member,
                                                                                    frequency = frequency,
                                                                                    table_id = filename_spesification,
                                                                                    source_id = model,
                                                                                    )

                            if not isinstance(ds, xr.Dataset):
                                print(f'{node_availability} returned empty datasets')

                            else:
                                node_availability = True
                                working_node_url = node_url

                        except Exception as e:
                            print('\n', f'Could not extract available ensamble members from {node_url}: {e}', '\n')

                        if node_url == node_urls[-1]:
                            out_of_nodes = True
                            print(f'out_of_nodes is set to {out_of_nodes}')
                        else:
                            i += 1
                            node_url = node_urls[i]


                    print('\n')
                    if node_availability == True:
                        node_url = working_node_url
                        print(f'Managed to find the opendap_url(s) and open the dataset with the data node: {node_url}')
                    else:
                        print(f'Did not manage to find the opendap urls within either of the following data nodes: {nodes}')
                        sys.exit()
                    
                    calculateNprepare_siareanNsiextentn(
                                                                                            
                                                        dataset = ds,
                                                        siarean_path = siarean_path+f'siarean_{filename_spesification}_{model}_{scenario}_{ensamble_member}_2015_2100.nc',
                                                        siextentn_path = siextentn_path+f'siextentn_{filename_spesification}_{model}_{scenario}_{ensamble_member}_2015_2100.nc',
                                                        siarean_maxmin_path = siarean_maxmin_path+f'siarean_{filename_spesification}_{model}_{scenario}_{ensamble_member}_2015_2100.nc',
                                                        siextentn_maxmin_path = siextentn_maxmin_path+f'siextentn_{filename_spesification}_{model}_{scenario}_{ensamble_member}_2015_2100.nc',
                                                        model = model,
                                                        scenario = scenario,
                                                        ensamble_member = ensamble_member,
                                                        node_urls = node_urls,

                                                                                )
                    
                print('\n')