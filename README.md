# FrEDI_NPD

This repository includes the input data, analysis scripts, and aggregate output sectoral damages data associated with
Hartin C. et al., (in prep) "Advancing the estimation of future climate impacts within the United States".

Corresponding Author: Corinne Hartin (hartin.corinne@epa.gov)

Plain Language Summary:
This study utilizes the reduced-complexity model, Framework for Evaluating Damages and Impacts (FrEDI), to assess the impacts and net present damages (NPD) from climate change to the United States across 10,000 future probabilistic emission and socioeconomic projections.

The structure of the repository is as follows:
/input
     - inlcudes input data from the RFF-SP dataset (https://zenodo.org/record/6016583#.Y9GpuXbMKbg) for population and GDP. 
     - temperature data were produced from the FaIR simple climate model (https://github.com/OMS-NetZero/FAIR)

/code
     - scripts to 
	(1) pre-process the RFF-SP and temperature input data for use in the EPA FrEDI model
	(2) run the FrEDI model (FrEDI source code avaialble at: https://github.com/USEPA/FrEDI/releases/tag/FrEDI_2300
	(3) compute net present damages (NPD) from FrEDI
	(4) post-process to chunk raw FrEDI results into smaller files and create summary statistics for manuscript figures
     - scripts to run SSP input data in FrEDI

/output 
     - contains FrEDI output (used to create manuscript figures)

/analysis
     - contains the .Rmd used to generate all numerical values and figures for the manuscript.

