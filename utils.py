import xarray as xr
import numpy as np 
import pandas as pd
import dask
import os
from QueryESGF4RequiredVariables import query_and_open_OPeNDAP_urls_from_ESGF

import logging
import sys

import ast

def get_script_root_path():
    # Get the absolute path of the script and then extract the directory
    return os.path.dirname(os.path.abspath(__file__))

def read_config_file(file_path, 
                     variables_as_lists = {"models", "temporal_resolutions", "scenarios", "nodes"}  # Variables to always treat as lists of strings
                     ):
    """
    Reads a configuration file where each line contains a variable and its content in the format:
    variable: content

    :param file_path: Path to the configuration file
    :return: Dictionary with variable names as keys and their content as values
    """
    config = {}
    try:
        with open(file_path, 'r') as file:
            for line in file:
                # Strip any leading/trailing whitespace and ignore empty lines
                line = line.strip()
                # Skip empty lines or lines starting with '#'
                if not line or line.startswith('#'):
                    continue
                if line and ':' in line:  # Ensure the line contains a colon
                    # Split into variable and content, stripping extra spaces around them
                    variable, content = map(str.strip, line.split(':', 1))
                    
                    # Handle specific variables that must be lists of strings
                    if variable in variables_as_lists:
                        parsed_content = [item.strip() for item in content.split(',')]
                    # Fallback: Handle variables with comma-separated values
                    elif ',' in content:
                        parsed_content = [item.strip() for item in content.split(',')]
                    else:
                        # Try to parse the content as a Python literal (e.g., a list, number, etc.)
                        try:
                            parsed_content = ast.literal_eval(content)
                        except (ValueError, SyntaxError):
                            # If parsing fails, keep the content as a string
                            parsed_content = content
                    
                    # Add the parsed variable and content to the dictionary
                    config[variable] = parsed_content
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except Exception as e:
        print(f"An error occurred while reading the file: {e}")
    
    return config


def init_logging():
    # Log to console
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    log_info = logging.StreamHandler(sys.stdout)
    log_info.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(log_info)
    return logger
#logger = init_logging()



def calculateNprepare_siareanNsiextentn(dataset, siarean_path, siextentn_path, siarean_maxmin_path, siextentn_maxmin_path, model, scenario, ensamble_member, node_urls, yearly_max_min = False):#, MIROC6_model = False, NorESM_model = False, CanESM5_model = False):
    
    node_availability = False
    i = 0
    node_url = node_urls[0]
    while (node_availability == False) and (node_url != 'https://not_a_node/esg-search'):
        
        try:
            area_ds = query_and_open_OPeNDAP_urls_from_ESGF(node_url = node_url,
                                                            project = 'CMIP6',
                                                            experiment_id = scenario,
                                                            variable_id = 'areacello',
                                                            variant_label = ensamble_member,
                                                            frequency = None,
                                                            table_id = None,
                                                            source_id = model,
            )

            if not isinstance(area_ds, xr.Dataset):
                print(f'{node_url} returned empty query for the spatial resolution dataset.')
                                
            else:
                node_availability = True
                working_node_url = node_url

        # except UnboundLocalError: # If opendap_url won't open
        except Exception as e:
            print(f'Could not extract spatial dataset from {node_url}: {e}')

        i += 1
        node_url = node_urls[i]

    print('\n')
    if node_availability == True:
        node_url = working_node_url
        print(f'Managed to find the spatial dataset on the data node {node_url}:')
    else:
        print(f'Did not manage to find the opendap urls within either of the following data nodes: {nodes}')
        return
    
    if dataset.source_id == 'NorESM2-LM' or dataset.source_id == 'CanESM5':
        #''' CanESM5 AND NorESM2-LMconverter of time coordinate and time_bnds variable
        # Convert cftime.DatetimeNoLeap to np.datetime64 - For CanESM5
        time_dim_np = pd.to_datetime(dataset.time.values.astype(str))
        dataset['time'] = time_dim_np

        dataset['time'].attrs['standard_name'] = 'time'
        dataset['time'].attrs['long_name'] = 'time'

        # Create a copy of the time_bnds as a numpy array
        time_bnds_np = dataset['time_bnds'].values

        # Convert the elements to datetime64
        time_bnds_np[:, 0] = pd.to_datetime(time_bnds_np[:, 0].astype(str))
        time_bnds_np[:, 1] = pd.to_datetime(time_bnds_np[:, 1].astype(str))

        # Create a new xarray DataArray and replace the old time_bnds
        dataset['time_bnds'] = xr.DataArray(time_bnds_np, dims=dataset['time_bnds'].dims, coords=dataset['time_bnds'].coords)
        #'''

    lat_cut_off = 45

    if dataset.source_id == 'MIROC6' or dataset.source_id == 'MRI-ESM2-0':
        # List of dimensions or variables to keep
        allowed_dims = ['time', 'y', 'x']  # Only these variables will be loaded
        
        chunks={'time':500, 'y':-1, 'x':-1} # This is done in the query_ESGF function
        area_chunks={'y':-1, 'x':-1}

    else:
         chunks = {'time':500, 'j':-1, 'i':-1} # This is done in the query_ESGF function
         area_chunks={'j':-1, 'i':-1}
    
    siconc = dataset.siconc.chunk(chunks)
    siconc_cropped = siconc.where(siconc.latitude >= lat_cut_off)#, drop=True) # Doesn't work to drop when chunking - and chunking is necessary to maintain under "streaming restrictions" 
    areacello = area_ds.areacello.chunk(area_chunks)
    print(f'siconc_cropped: {siconc_cropped}')


    if dataset.source_id == 'NorESM2-LM':

        grid_area = areacello.where(area_ds.latitude >= lat_cut_off)#, drop=True)
        grid_area = grid_area.isel(j=slice(0,-1))/1e12    # 1e6 km2  (Drop the last j point in NorESM when looking at sea ice variables: https://noresm-docs.readthedocs.io/en/noresm2/faq/postp_plotting_faq.html)

    else:
        grid_area = areacello.where(area_ds.latitude >= lat_cut_off)#, drop=True)
        grid_area = grid_area/1e12    # 1e6 km2 and only account for the Northern Hemisphere
    print(f'grid_area: {grid_area}')

    
    # Mask every grid cell below 45 degrees north and where siconc < 15% and siconc < 0%, respectively
    Variable_north = siconc_cropped#.where(dataset.latitude>=lat_cut_off)#, drop=True)   # only consider siconc above lat = 45 - Arctic I set 0 to actually account for the entire Northern Hemisphere
    Variable_0 = Variable_north.where(Variable_north > 0)#.chunk(chunks)               # Only want to keep grid cells with values above 0%
    Variable_15 = Variable_north.where(Variable_north > 15)#.chunk(chunks)               # Only want to keep grid cells with values above 15%

    print('Starting the siarean computation')

    # print(f'The evaluated grid size used in this calculation is the one of {str(area_ds)}.')
    if dataset.source_id == 'MIROC6' or dataset.source_id == 'MRI-ESM2-0':
        siarean_variable = ((grid_area.where(Variable_0 > 0)*(Variable_north/100)).sum(dim=['x','y']).astype('float64')).compute()  # sum of area where siconc > 0% and lat > 60 degrees north
    else:
        siarean_variable = ((grid_area.where(Variable_0 > 0)*(Variable_north/100)).sum(dim=['i','j']).astype('float64')).compute()  # sum of area where siconc > 0% and lat > 60 degrees north

    # print('Starting the siarean and siextentn computation')

    # Compute siarean_variable in chunks
    # siarean_variable_computing= dask.compute(siarean_variable, scheduler='synchronous')[0]
    # siarean_variable_computed = siarean_variable_computing.astype('float64').compute()
    # siarean_variable_computed = siarean_variable.astype('float64')
    siarean_variable_computed = siarean_variable
    

    # print(f'Maximum siarean is: {float(siarean_computed.max())} * 1e6 km2.')
    
    print('Starting the siextentn computation')

    if dataset.source_id == 'MIROC6' or dataset.source_id == 'MRI-ESM2-0':
        siextentn_variable = (grid_area.where(Variable_15 > 15).sum(dim=['x','y']).astype('float64')).compute()  # sum of area where siconc > 0% and lat > 60 degrees north
    else:
        siextentn_variable = (grid_area.where(Variable_15 > 15).sum(dim=['i','j']).astype('float64')).compute()  # sum of area where siconc > 0% and lat > 60 degrees north

    # Compute siextentn_variable in chunks
    # siextentn_variable_computing = dask.compute(siextentn_variable, scheduler='synchronous')[0]
    # siextentn_variable_computed = siextentn_variable_computing.astype('float64').compute()
    # siextentn_variable_computed = siextentn_variable.astype('float64')
    siextentn_variable_computed = siextentn_variable

    # print(f'Maximum siextentn is: {float(siextentn_computed.max())} * 1e6 km2.')
    
    
    ### Free up some memory
    del Variable_north, Variable_0, Variable_15

    
    ### Yearly max min calculation and prepping the results before writing to file
    siarean_yearly_max = siarean_variable_computed.resample(time='1YE').max()
    siarean_yearly_min = siarean_variable_computed.resample(time='1YE').min()

    siarean_maxmin_ds = xr.Dataset({
        'siarean_min': siarean_yearly_min,
        'siarean_max': siarean_yearly_max
        })

    # Add the gloabal attributes of the original dataset(s)
    siarean_maxmin_ds.attrs = dataset.attrs

    # Set attributes for the new variable
    siarean_maxmin_ds['siarean_min'].attrs['standard_name'] = 'sea_ice_area'
    siarean_maxmin_ds['siarean_min'].attrs['long_name'] = 'Yearly Minimum Arctic Sea Ice Area'
    siarean_maxmin_ds['siarean_min'].attrs['units'] = '1e6 km2'
    siarean_maxmin_ds['siarean_min'].attrs['description'] = 'Sea Ice Area (SIA) is the total ocean area covered by any amount of ice (no SIC threshold applied, and the SIC value weights the grid cell area). This is the yearly minimum Arctic sea ice area.'

    siarean_maxmin_ds['siarean_max'].attrs['standard_name'] = 'sea_ice_area'
    siarean_maxmin_ds['siarean_max'].attrs['long_name'] = 'Yearly Maximum Arctic Sea Ice Area'
    siarean_maxmin_ds['siarean_max'].attrs['units'] = '1e6 km2'
    siarean_maxmin_ds['siarean_max'].attrs['description'] = 'Sea Ice Area (SIA) is the total ocean area covered by any amount of ice (no SIC threshold applied, and the SIC value weights the grid cell area). This is the yearly maximum Arctic sea ice area.'




    siextentn_yearly_max = siextentn_variable_computed.resample(time='1YE').max()
    siextentn_yearly_min = siextentn_variable_computed.resample(time='1YE').min()

    siextentn_maxmin_ds = xr.Dataset({
        'siextentn_min': siextentn_yearly_min,
        'siextentn_max': siextentn_yearly_max
        })

    # Add the gloabal attributes of the original dataset(s)
    siextentn_maxmin_ds.attrs = dataset.attrs

    # Set attributes for the new variable
    siextentn_maxmin_ds['siextentn_min'].attrs['standard_name'] = 'sea_ice_extent'
    siextentn_maxmin_ds['siextentn_min'].attrs['long_name'] = 'Yearly Minimum Arctic Sea Ice Extent'
    siextentn_maxmin_ds['siextentn_min'].attrs['units'] = '1e6 km2'
    siextentn_maxmin_ds['siextentn_min'].attrs['description'] = 'Sea Ice Extent (SIE) is defined as the area covered by a significant amount of sea ice, that is the area of ocean covered with more than 15% Sea Ice Concentration (SIC). This is the yearly minimum Arctic sea ice extent.'

    siextentn_maxmin_ds['siextentn_max'].attrs['standard_name'] = 'sea_ice_extent'
    siextentn_maxmin_ds['siextentn_max'].attrs['long_name'] = 'Yearly Maximum Arctic Sea Ice Extent'
    siextentn_maxmin_ds['siextentn_max'].attrs['units'] = '1e6 km2'
    siextentn_maxmin_ds['siextentn_max'].attrs['description'] = 'Sea Ice Extent (SIE) is defined as the area covered by a significant amount of sea ice, that is the area of ocean covered with more than 15% Sea Ice Concentration (SIC). This is the yearly maximum Arctic sea ice extent.'

    # Define encoding for the time variable to store as int32
    reference_date = np.datetime64('1970-01-01')
    maxmin_encoding = {
        'time': {'units': 'days since 1970-01-01',
                    'calendar': 'standard',
                    'dtype': 'int32',
                    }
                }

    # Save to NetCDF files - to avoid permission error remove existing first before writing
    if os.path.exists(siarean_maxmin_path):
                os.remove(siarean_maxmin_path)

    siarean_maxmin_ds.to_netcdf(path = siarean_maxmin_path, encoding = maxmin_encoding)

    if os.path.exists(siextentn_maxmin_path):
                os.remove(siextentn_maxmin_path)

    siextentn_maxmin_ds.to_netcdf(path = siextentn_maxmin_path, encoding = maxmin_encoding)


    # Calculate the variables on the original time resolution and prepping the results before writing to file
    siarean_variable_computed = siarean_variable_computed.to_dataset(name ='siarean')   # converting xarray.DataArray to xarray.Dataset
    siextentn_variable_computed = siextentn_variable_computed.to_dataset(name = 'siextentn')  # converting xarray.DataArray to xarray.Dataset

    #### Adding sea ice area
    # Add the gloabal attributes of the original dataset(s)
    siarean_variable_computed.attrs = dataset.attrs

    # Set attributes for the new variable
    siarean_variable_computed.siarean.attrs['standard_name'] = 'sea_ice_area'
    siarean_variable_computed.siarean.attrs['long_name'] = 'Arctic Sea Ice Area'
    siarean_variable_computed.siarean.attrs['units'] = '1e6 km2'
    siarean_variable_computed.siarean.attrs['description'] = 'Sea Ice Area (SIA) is the total ocean area covered by any amount of ice (no SIC threshold applied, and the SIC value weights the grid cell area). This is the Arctic sea ice area.'


    #### Adding sea ice extent
    # Add the computed sea ice area to the dataset as a new variable
    siextentn_variable_computed.attrs = dataset.attrs

    # Set attributes for the new variable
    siextentn_variable_computed.siextentn.attrs['standard_name'] = 'sea_ice_extent'
    siextentn_variable_computed.siextentn.attrs['long_name'] = 'Arctic Sea Ice Extent'
    siextentn_variable_computed.siextentn.attrs['units'] = '1e6 km2'
    siextentn_variable_computed.siextentn.attrs['description'] = 'Sea Ice Extent (SIE) is defined as the area covered by a significant amount of sea ice, that is the area of ocean covered with more than 15% Sea Ice Concentration (SIC). This is the Arctic sea ice extent.'

    # Define encoding for the time variable to store as int32
    reference_date = np.datetime64('1970-01-01')
    encoding = {'time': {'units': f'hours since {reference_date}',
                        'calendar': 'standard',
                        'dtype': 'int32',
                        # 'zlib': True  # Optional: compress the data
                    }
            }
        
    ''' Change from 'units': f'days since {reference_date}' to 'units': f'hours since {reference_date}'
    Because of UserWarning: Times can't be serialized faithfully to int64 with requested units 'days since 1970-01-01'. Serializing with units 'hours since 1970-01-01' instead. Set encoding['dtype'] to floating point dtype to serialize with units 'days since 1970-01-01'. Set encoding['units'] to 'hours since 1970-01-01' to silence this warning .
                            siextentn_variable_computed.to_netcdf(path = siextentn_path, encoding = encoding)
    '''

    # Save to NetCDF files - to avoid permission error remove existing first before writing
    if os.path.exists(siarean_path):
                os.remove(siarean_path)

    siarean_variable_computed.to_netcdf(path = siarean_path, encoding = encoding)

    if os.path.exists(siextentn_path):
                os.remove(siextentn_path)

    siextentn_variable_computed.to_netcdf(path = siextentn_path, encoding = encoding)

    print('Calculations are finished and the results are written to NetCDF!')

    return #siarean_variable_computed, siextentn_variable_computed, siarean_maxmin_dataset, siextentn_maxmin_ds


def alter_siarean_and_siextentn(siarean_dataset, siextentn_dataset, siarean_path, siextentn_path):

    if siarean_dataset.source_id == 'NorESM2-LM' or siarean_dataset.source_id == 'CanESM5':
        #''' CanESM5 AND NorESM2-LMconverter of time coordinate and time_bnds variable
        # Convert cftime.DatetimeNoLeap to np.datetime64 - For CanESM5
        time_dim_np = pd.to_datetime(siarean_dataset.time.values.astype(str))
        siarean_dataset['time'] = time_dim_np

        siarean_dataset['time'].attrs['standard_name'] = 'time'
        siarean_dataset['time'].attrs['long_name'] = 'time'

        # Create a copy of the time_bnds as a numpy array
        time_bnds_np = siarean_dataset['time_bnds'].values

        # Convert the elements to datetime64
        time_bnds_np[:, 0] = pd.to_datetime(time_bnds_np[:, 0].astype(str))
        time_bnds_np[:, 1] = pd.to_datetime(time_bnds_np[:, 1].astype(str))

        # Create a new xarray DataArray and replace the old time_bnds
        siarean_dataset['time_bnds'] = xr.DataArray(time_bnds_np, dims=siarean_dataset['time_bnds'].dims, coords=siarean_dataset['time_bnds'].coords)
        #'''

    if siextentn_dataset.source_id == 'NorESM2-LM' or siextentn_dataset.source_id == 'CanESM5':
        #''' CanESM5 AND NorESM2-LMconverter of time coordinate and time_bnds variable
        # Convert cftime.DatetimeNoLeap to np.datetime64 - For CanESM5
        time_dim_np = pd.to_datetime(siextentn_dataset.time.values.astype(str))
        siextentn_dataset['time'] = time_dim_np

        siextentn_dataset['time'].attrs['standard_name'] = 'time'
        siextentn_dataset['time'].attrs['long_name'] = 'time'

        # Create a copy of the time_bnds as a numpy array
        time_bnds_np = siextentn_dataset['time_bnds'].values

        # Convert the elements to datetime64
        time_bnds_np[:, 0] = pd.to_datetime(time_bnds_np[:, 0].astype(str))
        time_bnds_np[:, 1] = pd.to_datetime(time_bnds_np[:, 1].astype(str))

        # Create a new xarray DataArray and replace the old time_bnds
        siextentn_dataset['time_bnds'] = xr.DataArray(time_bnds_np, dims=siextentn_dataset['time_bnds'].dims, coords=siextentn_dataset['time_bnds'].coords)
        #'''
        
    # Extract the two variables
    siarean = siarean_dataset.siarean.astype('float32')

    siextentn = siextentn_dataset.siextentn.astype('float32')


    # siarean

    siarean_ds = xr.Dataset({
    'siarean': siarean
    })

    # Add the gloabal attributes of the original dataset(s)
    siarean_ds.attrs = siarean_dataset.attrs

    # Set attributes for the new variable
    siarean_ds['siarean'].attrs['standard_name'] = 'sea_ice_area'
    siarean_ds['siarean'].attrs['long_name'] = 'Arctic Sea Ice Area'
    siarean_ds['siarean'].attrs['units'] = '1e6 km2'
    siarean_ds['siarean'].attrs['description'] = 'Sea Ice Area (SIA) is the total ocean area covered by any amount of ice (no SIC threshold applied, and the SIC value weights the grid cell area). This is the Arctic sea ice area.'


    # siextentn

    siextentn_ds = xr.Dataset({
    'siextentn': siextentn
    })

    # Add the gloabal attributes of the original dataset(s)
    siextentn_ds.attrs = siextentn_dataset.attrs

    # Set attributes for the new variable
    siextentn_ds['siextentn'].attrs['standard_name'] = 'sea_ice_extent'
    siextentn_ds['siextentn'].attrs['long_name'] = 'Arctic Sea Ice Extent'
    siextentn_ds['siextentn'].attrs['units'] = '1e6 km2'
    siextentn_ds['siextentn'].attrs['description'] = 'Sea Ice Extent (SIE) is defined as the area covered by a significant amount of sea ice, that is the area of ocean covered with more than 15% Sea Ice Concentration (SIC). This is the Arctic sea ice extent.'

    # Define encoding for the time variable to store as int32
    reference_date = np.datetime64('1970-01-01')
    encoding = {'time': {'units': f'hours since {reference_date}',
                         'calendar': 'standard',
                         'dtype': 'int32',
                         # 'zlib': True  # Optional: compress the data
                        }
                }
    
    ''' Change from 'units': f'days since {reference_date}' to 'units': f'hours since {reference_date}'
    Because of UserWarning: Times can't be serialized faithfully to int64 with requested units 'days since 1970-01-01'. Serializing with units 'hours since 1970-01-01' instead. Set encoding['dtype'] to floating point dtype to serialize with units 'days since 1970-01-01'. Set encoding['units'] to 'hours since 1970-01-01' to silence this warning .
                            siextentn_variable_computed.to_netcdf(path = siextentn_path, encoding = encoding)
    '''

    # Save to NetCDF files - to avoid permission error remove existing first before writing
    if os.path.exists(siarean_path):
            os.remove(siarean_path)

    siarean_ds.to_netcdf(path = siarean_path, encoding = encoding)

    if os.path.exists(siextentn_path):
            os.remove(siextentn_path)

    siextentn_ds.to_netcdf(path = siextentn_path, encoding = encoding)

    return siarean_ds, siextentn_ds



def siarean_and_siextentn2yearly_max_min(siarean_dataset, siextentn_dataset, siarean_path, siextentn_path):

    if siarean_dataset.source_id == 'NorESM2-LM' or siarean_dataset.source_id == 'CanESM5':
        #''' CanESM5 AND NorESM2-LMconverter of time coordinate and time_bnds variable
        # Convert cftime.DatetimeNoLeap to np.datetime64 - For CanESM5
        time_dim_np = pd.to_datetime(siarean_dataset.time.values.astype(str))
        siarean_dataset['time'] = time_dim_np

        siarean_dataset['time'].attrs['standard_name'] = 'time'
        siarean_dataset['time'].attrs['long_name'] = 'time'

        # Create a copy of the time_bnds as a numpy array
        time_bnds_np = siarean_dataset['time_bnds'].values

        # Convert the elements to datetime64
        time_bnds_np[:, 0] = pd.to_datetime(time_bnds_np[:, 0].astype(str))
        time_bnds_np[:, 1] = pd.to_datetime(time_bnds_np[:, 1].astype(str))

        # Create a new xarray DataArray and replace the old time_bnds
        siarean_dataset['time_bnds'] = xr.DataArray(time_bnds_np, dims=siarean_dataset['time_bnds'].dims, coords=siarean_dataset['time_bnds'].coords)
        #'''

    if siextentn_dataset.source_id == 'NorESM2-LM' or siextentn_dataset.source_id == 'CanESM5':
        #''' CanESM5 AND NorESM2-LMconverter of time coordinate and time_bnds variable
        # Convert cftime.DatetimeNoLeap to np.datetime64 - For CanESM5
        time_dim_np = pd.to_datetime(siextentn_dataset.time.values.astype(str))
        siextentn_dataset['time'] = time_dim_np

        siextentn_dataset['time'].attrs['standard_name'] = 'time'
        siextentn_dataset['time'].attrs['long_name'] = 'time'

        # Create a copy of the time_bnds as a numpy array
        time_bnds_np = siextentn_dataset['time_bnds'].values

        # Convert the elements to datetime64
        time_bnds_np[:, 0] = pd.to_datetime(time_bnds_np[:, 0].astype(str))
        time_bnds_np[:, 1] = pd.to_datetime(time_bnds_np[:, 1].astype(str))

        # Create a new xarray DataArray and replace the old time_bnds
        siextentn_dataset['time_bnds'] = xr.DataArray(time_bnds_np, dims=siextentn_dataset['time_bnds'].dims, coords=siextentn_dataset['time_bnds'].coords)
        #'''
        
    # Extract the two variables
    siarean = siarean_dataset.siarean
    siarean = siarean.astype('float32')

    siextentn = siextentn_dataset.siextentn
    siextentn = siextentn.astype('float32')


    # siarean

    siarean_yearly_max = siarean.resample(time='1YE').max()
    siarean_yearly_min = siarean.resample(time='1YE').min()

    siarean_ds = xr.Dataset({
    'siarean_min': siarean_yearly_min,
    'siarean_max': siarean_yearly_max
    })

    # Add the gloabal attributes of the original dataset(s)
    siarean_ds.attrs = siarean_dataset.attrs

    # Set attributes for the new variable
    siarean_ds['siarean_min'].attrs['standard_name'] = 'sea_ice_area'
    siarean_ds['siarean_min'].attrs['long_name'] = 'Yearly Minimum Arctic Sea Ice Area'
    siarean_ds['siarean_min'].attrs['units'] = '1e6 km2'
    siarean_ds['siarean_min'].attrs['description'] = 'Sea Ice Area (SIA) is the total ocean area covered by any amount of ice (no SIC threshold applied, and the SIC value weights the grid cell area). This is the yearly minimum Arctic sea ice area.'

    siarean_ds['siarean_max'].attrs['standard_name'] = 'sea_ice_area'
    siarean_ds['siarean_max'].attrs['long_name'] = 'Yearly Maximum Arctic Sea Ice Area'
    siarean_ds['siarean_max'].attrs['units'] = '1e6 km2'
    siarean_ds['siarean_max'].attrs['description'] = 'Sea Ice Area (SIA) is the total ocean area covered by any amount of ice (no SIC threshold applied, and the SIC value weights the grid cell area). This is the yearly maximum Arctic sea ice area.'


    # siextentn

    siextentn_yearly_max = siextentn.resample(time='1YE').max()
    siextentn_yearly_min = siextentn.resample(time='1YE').min()

    siextentn_ds = xr.Dataset({
    'siextentn_min': siextentn_yearly_min,
    'siextentn_max': siextentn_yearly_max
    })

    # Add the gloabal attributes of the original dataset(s)
    siextentn_ds.attrs = siextentn_dataset.attrs

    # Set attributes for the new variable
    siextentn_ds['siextentn_min'].attrs['standard_name'] = 'sea_ice_extent'
    siextentn_ds['siextentn_min'].attrs['long_name'] = 'Yearly Minimum Arctic Sea Ice Extent'
    siextentn_ds['siextentn_min'].attrs['units'] = '1e6 km2'
    siextentn_ds['siextentn_min'].attrs['description'] = 'Sea Ice Extent (SIE) is defined as the area covered by a significant amount of sea ice, that is the area of ocean covered with more than 15% Sea Ice Concentration (SIC). This is the yearly minimum Arctic sea ice extent.'

    siextentn_ds['siextentn_max'].attrs['standard_name'] = 'sea_ice_extent'
    siextentn_ds['siextentn_max'].attrs['long_name'] = 'Yearly Maximum Arctic Sea Ice Extent'
    siextentn_ds['siextentn_max'].attrs['units'] = '1e6 km2'
    siextentn_ds['siextentn_max'].attrs['description'] = 'Sea Ice Extent (SIE) is defined as the area covered by a significant amount of sea ice, that is the area of ocean covered with more than 15% Sea Ice Concentration (SIC). This is the yearly maximum Arctic sea ice extent.'

    # Define encoding for the time variable to store as int32
    reference_date = '1970-01-01'
    encoding = {'time': {'units': f'hours since {reference_date}',
                         'calendar': 'standard',
                         'dtype': 'int32',
                         # 'zlib': True  # Optional: compress the data
                        }
                }
    
    ''' Change from 'units': f'days since {reference_date}' to 'units': f'hours since {reference_date}'
    Because of UserWarning: Times can't be serialized faithfully to int64 with requested units 'days since 1970-01-01'. Serializing with units 'hours since 1970-01-01' instead. Set encoding['dtype'] to floating point dtype to serialize with units 'days since 1970-01-01'. Set encoding['units'] to 'hours since 1970-01-01' to silence this warning .
                            siextentn_variable_computed.to_netcdf(path = siextentn_path, encoding = encoding)
    '''

    # Save to NetCDF files - to avoid permission error remove existing first before writing
    if os.path.exists(siarean_path):
            os.remove(siarean_path)

    siarean_ds.to_netcdf(path = siarean_path, encoding = encoding)

    if os.path.exists(siextentn_path):
            os.remove(siextentn_path)

    siextentn_ds.to_netcdf(path = siextentn_path, encoding = encoding)

    return siarean_ds, siextentn_ds