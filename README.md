# **CMIP6 Sea Ice Area and Sea Ice Extent as Climate Indicators**
This repository provides tools to extract **Sea Ice Area** and **Sea Ice Extent** from CMIP6 model scenarios, enabling the creation of Climate Indicators for the period spanning 2015–2100. These indicators can be used to assess and monitor climate trends based on specific model outputs, temporal resolutions, and SSP scenarios.

---

## **Quick Overview**
This repository includes a Python script, Creating_Climate_Indicators.py, which processes CMIP6 data and generates Climate Indicators. The script relies on a user-defined configuration file (config.yaml) that specifies the models, scenarios, temporal resolutions, and other parameters required for data extraction and processing.

---

## **Usage**

### **1. Configure the `config.yaml` File**
Before running the script, create a `config.yaml` file tailored to your requirements. Below is an example template:

```yaml
models: MIROC6, CanESM5, EC-Earth3-Veg, NorESM2-LM, ACCESS-CM2, MRI-ESM2-0
temporal_resolutions: Daily, Monthly
scenarios: ssp585, ssp460, ssp370, ssp245, ssp126
nodes: esgf-node.llnl.gov, esg-dn1.nsc.liu.se, esgf.nci.org.au, vesg.ipsl.upmc.fr
indicator_output_path: /indicator/output/path
maxmin_indicator_output_path: /maxminindicator/output/path
indicator_output_path: /path/to/indicator/output
maxmin_indicator_output_path: /path/to/maxmin/indicator/output
``` 

### **2. Set Up the Environment**
This repository uses a `mamba` environment for managing dependencies. To set up the environment:

1. Ensure you have **mamba** installed. If not, install it using `conda`:

```bash
conda install mamba -n base -c conda-forge
```

2. Create the environment using the provided `environment.yml` file:

```bash
mamba env create -f environment.yml
```

3. Activate the environment:

```bash
mamba activate ClimateIndicators_env
```

Here the mamba environment name (ClimateIndicators_env) is defined in the environment.yml.

### **3. Run the Script**
Execute the script to generate Sea Ice Area and Sea Ice Extent indicators:

```bash
python Creating_Climate_Indicators.py
```

The script will:

* Extract the specified climate indicators from CMIP6 models based on temporal resolutions and SSP scenarios.
* Search and stream data from the selected ESGF-nodes.
* In cases where the indicators are not available, other variables (siconc and areacello) are streamed and used to compute the specified climate indicators. 
* Save the results as NetCDF files (.nc), as well as NetCDF files with yearly maximum and minimum indicator values.


## **Key Features**

### **1. Direct Variable Extraction**
If Sea Ice Area or Sea Ice Extent is directly available for a given model, scenario, and temporal resolution, the script extracts the variable directly from the first ESGF-node that has the variables in question.

### **2. Computation from Sea Ice Concentration**
For models where Sea Ice Area or Sea Ice Extent is not directly available, the script:

* Extracts Sea Ice Concentration and grid cell area data.
* Computes the indicators by applying necessary spatial calculations.
* Writes the results to NetCDF files for further analysis.

### **3. Flexible Node Selection**
Users can specify a list of ESGF (Earth System Grid Federation) nodes that in turn will be queried to find and stream the required data using OPeNDAP.

### **4. Configurable Output**
Results are stored in user-specified output directories:

* `indicator_output_path`: For processed Sea Ice Area and Sea Ice Extent indicators.
* `maxmin_indicator_output_path: For yearly maximum and minimum values for each indicator.

## **Computational Requirements**
Calculating indicators with **daily resolution** is computationally intensive and requires a significant amount of memory and CPU resources. It is advisable to allocate sufficient memory to your environment to handle these calculations efficiently.

For example:

* Ensure that your system or computational environment has at least 50–100 GB of memory (or more for larger datasets).
* Consider using a high-performance computing cluster if processing multiple models or scenarios simultaneously.

## **Requirements**
    
* Python 3.x
* Mamba or Conda for environment management

Install all dependencies using the environment.yml file as described above.