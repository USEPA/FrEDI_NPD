#####
## Title:   2_run_fredi_2300_ch4_n2O.R
## Purpose: This file will run FrEDI for all RFF sp scenarios, aggregate results,
##          and output FrEDI dataframe results for each individual scenario [trial].
##          Updated to run fredi with temperature trajectories from pulses of ch4 and n2o
## Inputs:  data/input_files/pop/pop_[scenario].csv
##          data/input_files/gdp/gdp_[scenario].csv
##          data/input_files/temp_baseline/temp_baseline_[scenario].csv
##          data/input_files/temp_perturbed/temp_perturbed_[scenario].csv\
##          output/damages/rffsp/trials.csv
## Outputs: output/damages/rffsp/damages_[scenario].parquet
## Written by: US EPA, National Center for Environmental Economics (OP) and Climate Change Division (OAP); January 2022
## Last updated: 4/19/2023 by E. McDuffie
#####
"Open SCC_FrEDI project first!!!" %>% print

###### Clear Workspace ######
rm(list = ls()); gc()


###### Install proper version of FrEDI ############
library("devtools")
branch = 'FrEDI_2300' #tagged version for Hartin paper
inst_flag = FALSE

if ( inst_flag == TRUE){
  remove.packages('FrEDI')
  withr::with_libpaths(
    new = .libPaths()[1],
    devtools::install_github(
      repo = "USEPA/FrEDI",
      subdir = "FrEDI",
      type = "source",
      force = TRUE,
      ref = branch
    ))
}


###### Load tidyverse and FrEDI ######
require(tidyverse); 
require(FrEDI)

###### Set Paths ######
inputsPath   <- "." %>% file.path("input", "input_files")
perturb_gas  <- c("ch4","n2o")
#perturb_gas  <- c("ch4")
perturb_year <- 2020
outputsPath  <- "." %>% file.path("output", "damages", "rffsp")

###### Set Print Options ######
options(digits = 20)

###### Load Packages ######
## This function will check if a package is installed and, if not, install it
list_of_packages <- c("FrEDI", "tidyverse", "magrittr", "cli",  "withr", "foreach","doParallel", "parallel", "arrow") %>%
  lapply(function(package_i, fredi_repo = fredi_branch){
    # installed_i  <- require(parse(text=package_i))
    installed_i  <- substitute(require(a), list(a = package_i)) %>% eval
    do_install_i <- !installed_i
    is_fredi     <- package_i == "FrEDI"
    ### If package is not installed, install it and try again
    if(do_install_i){
      ### Install procedure for FrEDI from Repo
      "Warning: package '" %>% paste0(package_i, "' not installed!") %>% message
      "\t" %>% paste0("Installing package from source...") %>% message
      if(is_fredi){
        # eem: already ensured correct version of FrEDI in step 0
      }
      ### Install procedure for CRAN packages
      else{
        try_install_i <- install.packages(package_i, repos = "http://cran.rstudio.com/") %>% try
      }
      ### After installing, try to load the package
      installed_i  <- require(package_i)
      do_install_i <- !installed_i
      if(do_install_i){
        "Warning: package '" %>% paste0(package_i, "' not installed.") %>% message
      }
    } ### End if do_install_i
  })


###### Set Selections ######
c_maxYear        <- 2300
c_selectYears    <- seq(2010, c_maxYear, 1)
c_selectModels   <- c("Average", "Interpolation")
c_aggLevels      <- c("impactyear", "national", "modelaverage")
c_dropCols       <- c("driverValue", "driverUnit", "driverType", "sectorprimary", "includeaggregate")
c_joinCols       <- c("sector", "variant", "impactYear", "impactType", "region", "model_type", "model", "year", "physicalmeasure", "gdp_usd", "national_pop", "gdp_percap", "reg_pop")

###### Set Runs ######
c_runs          <- 1:1e4

c_runs0 <- outputsPath %>%
  file.path("damages") %>%
  list.files(pattern = "\\.parquet") %>%
  (function(x){sub("\\.parquet", "", x)}) %>%
  (function(x){sub("damages_", "", x)}) %>%
  as.numeric %>%
  sort; 
c_runs0 <- outputsPath
c_runs0 %>% length

### Number of samples
n_samples <- 1#1e4
# c_runs1   <- c_runs0; c_runs1 %>% head; c_runs1 %>% length
c_runs1   <- c_runs0 %>% c(sample(
  x    = c_runs[!(c_runs %in% c_runs0)],
  size = n_samples - length(c_runs0)
)) %>% sort; c_runs1 %>% head; c_runs1 %>% length
### Save runs
if(length(c_runs1) > length(c_runs0)){data.frame(trial = c_runs1) %>% 
    write.csv(file = outputsPath %>% file.path("trials.csv"), row.names=F)}

###### Set Up Cluster ######
### Parallel filter and writing of feather files
### Detect cores and get number of cores
parallel::detectCores()
 n.cores    <- parallel::detectCores() - 1
#n.cores    <- 5
n.cores

### Make cluster
my.cluster <- parallel::makeCluster(n.cores, type = "PSOCK")

### Register cluster to be used by %dopar%
### Check if cluster is registered (optional)
### Check how many workers are available? (optional)
doParallel::registerDoParallel(cl = my.cluster); foreach::getDoParRegistered(); foreach::getDoParWorkers()

###### Run FrEDI ######
# Start the clock!
ptm   <- proc.time(); time1 <- Sys.time()

## start parallel
tmp_results <- foreach(
  # j = c_runs1[1:3],
  j = c_runs,
  .packages=c("tidyverse","FrEDI","arrow")
) %dopar% {
   #GAS      <- perturb_gas
   #YEAR     <- perturb_year
  for (GAS in perturb_gas) {
    for (YEAR in perturb_year) {
      ###### Set Paths #####
      suffix_j <- paste0(j, ".csv")
      ## file paths to socioeconomic input scenarios
      gdp.j  <- inputsPath %>% file.path("gdp", "gdp") %>% paste(suffix_j, sep="_")
      pop.j  <- inputsPath %>% file.path("pop", "pop") %>% paste(suffix_j, sep="_")
      
      ## baseline and perturbed temperature file
      temp.j.b <- inputsPath %>% file.path("temp_baseline", "temp_baseline") %>% paste(suffix_j, sep="_")
      temp.j.p <- inputsPath %>%
        file.path("temp_perturbed", GAS, "temp_perturbed") %>%
        paste(GAS, YEAR, suffix_j, sep="_")
      
      ###### Import Inputs #####
      ## input scenarios
      inputsList.b <- import_inputs(
        gdpfile  = gdp.j,
        popfile  = pop.j, popform   = "wide",
        tempfile = temp.j.b, temptype = "global"
      )
      
      ## input scenarios
      inputsList.p <- import_inputs(
        gdpfile  = gdp.j,
        popfile  = pop.j, popform   = "wide",
        tempfile = temp.j.p, temptype = "global"
      )
      
      ###### Run FrEDI #####
      ### Baseline temperature scenario
      results.b <- run_fredi(
        inputsList = inputsList.b,
        aggLevels  = c_aggLevels,
        thru2300   = TRUE,
        elasticity = 1) %>%
        mutate(trial = j)
      rm("inputsList.b")
      
      ### Perturbed temperature scenario
      results.p <- run_fredi(
        inputsList = inputsList.p,
        aggLevels  = c_aggLevels,
        thru2300   = TRUE,
        elasticity = 1) %>%
        mutate(trial = j)
      rm("inputsList.p")
      
      ### Bind Results
      results_i <- results.b %>% mutate(damageType = "Baseline") %>% 
        rbind( results.p %>% mutate(damageType = "Perturbed") )
      ### Output File
      outputsPath_j <- outputsPath
      if(length(perturb_gas) > 1 | length(perturb_year) > 1){
        outputsPath_j <- outputsPath_j %>% file.path(GAS)
      }
      output_file_i <- outputsPath_j %>%
        file.path("damages") %>%
        file.path("damages") %>%
        paste(j, sep="_") %>%
        paste("parquet", sep=".")
      ### Save results
      results_i %>% write_parquet(sink=output_file_i)
      rm("results_i", "output_file_i")
    } ### End perturb gas
  } ### End perturb year
}; rm("tmp_results")

###### Time ######
### stop the clock\
time2 <- Sys.time(); time2 - time1
proc.time() - ptm
###### Finish #####
### stop cluster
parallel::stopCluster(cl = my.cluster)

### END OF SCRIPT. Have a great day!