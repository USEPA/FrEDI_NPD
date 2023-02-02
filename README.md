# FrEDI_NPD

This repository includes the input data, analysis scripts, and output data and figures associated with<br>
Hartin C. et al., (in discussion) <em>"Advancing the estimation of future climate impacts within the United States".</em>

<strong>Corresponding Author</strong>: Corinne Hartin (hartin.corinne@epa.gov)

<strong>Plain Language Summary:</strong>
This study utilizes the reduced-complexity model, Framework for Evaluating Damages and Impacts (FrEDI), to assess the impacts and net present damages (NPD) from climate change in the United States, across 10,000 future probabilistic emission and socioeconomic projections.

----------------
The structure of the repository is as follows:<br>

/input
<ul>- includes input data from the RFF-SP dataset (https://zenodo.org/record/6016583#.Y9GpuXbMKbg) for population and GDP.<br>
     - temperature data were produced from the FaIR simple climate model (https://github.com/OMS-NetZero/FAIR)<br>
</ul>

/code
<ul> - scripts to:
	<ul>
	(1) pre-process the RFF-SP and temperature input data for use in the EPA FrEDI model<br>
	(2) run the FrEDI model (FrEDI source code available at: https://github.com/USEPA/FrEDI/releases/tag/FrEDI_2300<br>
	(3) compute net present damages (NPD) from FrEDI<br>
	(4) post-process to chunk raw FrEDI results into smaller files and create summary statistics for manuscript figures<br>
	</ul>
	- scripts to run SSP input data in FrEDI<br>
</ul>

/output <br>
<ul>- contains FrEDI output (used to create manuscript figures)</ul>

/analysis
<ul>- contains the .Rmd used to generate all numerical values and figures for the manuscript.</ul>

