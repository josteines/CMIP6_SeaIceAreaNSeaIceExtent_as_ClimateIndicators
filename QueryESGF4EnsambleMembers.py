from pyesgf.search import SearchConnection

def query_ESGF_nodes_4_ensable_members(node_url,
                                          project,
                                          experiment_id,
                                          variable_id,
                                          frequency,
                                          table_id,
                                          source_id,
                                          ):

    conn = SearchConnection(node_url, distrib=True) 

    # Define the facets you want to include in the search
    facets_of_interest = 'project,experiment_id,variable_id,variant_label,frequency,table_id,source_id, version, data_node'
    ctx = conn.new_context(
                           project=project,  # Specify the project (e.g., CMIP6, CMIP5)
                           experiment_id=experiment_id,  # Specify the experiment (e.g., historical, ssp585)
                           variable_id=variable_id,  # Specify the variable (e.g., siarean, siextentn, siconc)
                           frequency=frequency,  # Specify the frequency (e.g., mon, day)
                           table_id=table_id,  # Table ID (e.g., SImon, SIday)
                           source_id=source_id,  # Specify the model (e.g., "MIROC6", "CanESM5", "EC-Earth3-Veg", "NorESM2-LM", "ACCESS-CM2", "MRI-ESM2-0")
                           latest='True',
                           facets=facets_of_interest,
                        )
    
    # help(ctx)
    print(ctx)

    print(ctx.facet_counts['variant_label'])
    if len(ctx.facet_counts['variant_label']) == 0:
        return None

    ensamble_dict = ctx.facet_counts['variant_label']
    sorted_members = sorted(ensamble_dict.keys())

    print(f'Available ensamble members: {sorted_members}')
    
    return sorted_members