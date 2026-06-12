from retry import retry
from pyesgf.search import SearchConnection
import xarray as xr

project='CMIP6'  # Specify the project (e.g., CMIP6, CMIP5)
experiment_id='ssp585'  # Specify the experiment (e.g., historical, ssp585, areacello)
variable_id='siconc'  # Specify the variable (e.g., siarean, siextentn, siconc)
variant_label='r1i1p1f1'
frequency='day'  # Specify the frequency (e.g., mon, day - None if area variable)
table_id='SIday'  # Table ID (e.g., SImon, SIday - None if area variable)
source_id='EC-Earth3-Veg'  # Specify the model (e.g., "MIROC6", "CanESM5", "EC-Earth3-Veg", "NorESM2-LM", "ACCESS-CM2", "MRI-ESM2-0")

# @retry(tries=3, delay=5)
def query_and_open_OPeNDAP_urls_from_ESGF(node_url,
                                          project,
                                          experiment_id,
                                          variable_id,
                                          variant_label,
                                          frequency,
                                          table_id,
                                          source_id,
                                          ):

    conn = SearchConnection(node_url, distrib=True)

    # Define the facets you want to include in the search
    facets_of_interest = 'project,experiment_id,variable_id,variant_label,frequency,table_id,source_id, version, data_node'#, from_timestamp, to_timestamp'#, data_node'#,start,stop,version'#,opendap_url' #,filename,version,opendap_url' ,realm,grid_label  # Adjust as needed
    ctx = conn.new_context(
                           project=project,  # Specify the project (e.g., CMIP6, CMIP5)
                           experiment_id=experiment_id,  # Specify the experiment (e.g., historical, ssp585)
                           variable_id=variable_id,  # Specify the variable (e.g., siarean, siextentn, siconc)
                           variant_label=variant_label, # Ensamble member (e.g r1i1p1f1)
                           frequency=frequency,  # Specify the frequency (e.g., mon, day)
                           table_id=table_id,  # Table ID (e.g., SImon, SIday)
                           source_id=source_id,  # Specify the model (e.g., "MIROC6", "CanESM5", "EC-Earth3-Veg", "NorESM2-LM", "ACCESS-CM2", "MRI-ESM2-0")
                           latest='True',
                           facets=facets_of_interest,
                        )
    
    if ctx.hit_count == 0:
        return None
    # print(ctx.facet_counts)#['version'])

    # print(ctx.facet_counts['variant_label'])

    # print(f"Hits: {ctx.hit_count}, Versions: {ctx.facet_counts['version']}")
    versions_dict = ctx.facet_counts['version']
    # print(f'versions_dict, len(versions_dict) = {versions_dict},{len(versions_dict)}')

    # Make sure that we work with the newest version
    if len(versions_dict) == 1:
        newest_version = next(iter(versions_dict.keys()))
    
    newest_version = 0
    for version in versions_dict.keys():
        if int(version) > int(newest_version):
            newest_version = version
            

    print(f'Versions_dict: {versions_dict}')
    print(f'Newest_version: {newest_version}')

    nodes_dict = ctx.facet_counts['data_node']
    print(nodes_dict)
    for data_node in nodes_dict.keys():
        try:
            # Search for datasets
            results = ctx.search()

            # Extract the newest version of the dataset in question
            if results:
                for result in results:
                    #Check each individual dataet_id:
                    print(result.dataset_id)

                    # Make sure that the dataset that is extracted is from the correct node
                    if data_node in result.dataset_id:
                        dataset = result
                
                fctx = dataset.file_context()

                files = fctx.search(batch_size=30, 
                                    ignore_facet_check=True,
                                    )

                # help(fctx)
                
                # Print the number of files retrieved
                print(f"Number of files retrieved: {len(files)}")

                import re

                def extract_last_two_numbers_from_string_without_spaces(s):
                    matches = re.findall(r'\d+', s)  # Find all groups of digits
                    if len(matches) >= 2:
                        return matches[-2], matches[-1]  # Return the second-to-last and last numbers
                    elif len(matches) == 1:
                        return None, matches[-1]  # If only one group of numbers exists, return it as the last one
                    else:
                        return None, None  # If no numbers are found, return None for both
                    
                
                def extract_between_last_two_slashes(s):
                    parts = s.rsplit("/", 2)  # Split the string from the right into at most 3 parts
                    if len(parts) >= 3:
                        print(parts[2])
                        return parts[-2]  # The second-to-last part
                    else:
                        return None  # Return None if there are not enough slashes


                # Extract the OPeNDAP urls from each file within the timespan between 2015-01-01 to 2100-12-31 - Also ensures extraction of URLS from the desired node (which contains OPeNDAP_urls)
                OPeNDAP_urls = []
                file_number = 1
                for file in files:
                    # help(file)
                    print(f'Filename of file number {file_number}: {file.filename}.')
                    if 'areacello' == variable_id: 
                        node = extract_between_last_two_slashes(node_url)

                        if node in node_url:
                            OPeNDAP_urls.append(file.opendap_url)
                            print(f'The OPeNDAP_url for file number {file_number} is {file.opendap_url}')

                    else:

                        second_to_last_number, last_number = extract_last_two_numbers_from_string_without_spaces(file.filename)
                        print(f'Timespan of file {file_number}: {second_to_last_number} - {last_number}')  # StartYearMonth EndYearMonth, e.g. 201501 210012
                        if len(second_to_last_number) == len(last_number) == 6:     # Instance where the dates in the filenames only contain year and month
                            if int(second_to_last_number) >= 201501 and int(last_number) <= 210012:
                                OPeNDAP_urls.append(file.opendap_url)
                                print(f'The OPeNDAP_url for file number {file_number} is {file.opendap_url}')
                        elif len(second_to_last_number) == len(last_number) == 8:   # Instance where the dates in the filenames contain year, month and day
                            if int(second_to_last_number) >= 20150101 and int(last_number) <= 21001231:
                                OPeNDAP_urls.append(file.opendap_url)
                                print(f'The OPeNDAP_url for file number {file_number} is {file.opendap_url}')
                    
                    file_number += 1

                # Higlight how many OPeNDAP_urls that are found - Then open the datasets with xarray dependet on how many urls that are found 

                if len(OPeNDAP_urls) == 0:
                    print('No OPenDAP urls to be found')
                    return
                
                elif len(OPeNDAP_urls) == 1 and '.nc' in OPeNDAP_urls[0]:
                    print(f'There is one OPenDAP url to be found with the data node {data_node}')
                    try:
                        ds = xr.open_dataset(f'{OPeNDAP_urls[0]}', chunks={'time':500})
                        print("Dataset loaded successfully:", ds)
                    except Exception as e:
                        print("Failed to load dataset:", e)
                    return ds
                
                elif len(OPeNDAP_urls) > 1 and '.nc' in OPeNDAP_urls[0]:
                    print(f'There are {len(OPeNDAP_urls)} OPenDAP urls to be found  with the data node {data_node}')
                    OPeNDAP_urls.sort()
                    print(OPeNDAP_urls)
                    try:
                        ds = xr.open_mfdataset(OPeNDAP_urls, chunks={'time':500})
                        print("Dataset loaded successfully:", ds)
                    except Exception as e:
                        print("Failed to load dataset:", e)
                    return ds
                
                else:
                    return None # As ds is not defined we should move on to the next node
            
        except Exception as e:
            print(f'Failed to open datasets, or could not find them - Errormessage:', e)
        
    print('\n')