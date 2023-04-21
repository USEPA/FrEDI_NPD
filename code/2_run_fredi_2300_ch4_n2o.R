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
### 2.4 hours for 200 runs
### 3.8 hours for 400 runs CH 11/07/2022 
#### 200 baseline and 200 perturbed
## Last updated: 4/19/2023 by E. McDuffie
## Notes:       2.4 hours for 200 runs
##              CURRENTLY SET TO RUN 10000 TRIALS
#####
"Open SCC_FrEDI project first!!!" %>% print

###### Clear Workspace ######
rm(list = ls()); gc()


###### Install proper version of FrEDI ############
library("devtools")
#branch = 'thru2300'
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

###### Other Options ######
run_test <- TRUE

# ###### Test Results1 ######
# ### Highlight lines 29-49 and paste over them with the following
# if(run_test){
#   test_sectors <- c("ATS Extreme Temperature", "Coastal Properties")
#   test_inputs  <- list(
#     ### Values above maximum values
#     max = list(
#       tempInput = data.frame( year = 2000:2300, temp_C = 11),
#       slrInput  = data.frame( year = 2000:2300, slr_cm = 260)
#     ),
#     ### Values below minimum
#     min = list(
#       tempInput = data.frame(year = 2000:2020, temp_C = 1) %>% 
#         rbind(data.frame(year=2021:2300, temp_C = -1))
#     ),
#     ### Continuous function
#     oth = list(
#       tempInput = data.frame( year = 2000:2300, temp_C = seq(0, 11 , length.out=301)),
#       slrInput  = data.frame( year = 2000:2300, slr_cm = seq(0, 270, length.out=301))
#     ),
#     osc = import_inputs(
#       tempfile  = inputsPath %>% file.path("temp_baseline", "temp_baseline") %>% paste(768, sep="_") %>% paste0(".csv"),
#       gdpfile  = inputsPath %>% file.path("gdp", "gdp") %>% paste(768, sep="_") %>% paste0(".csv"),
#       popfile  = inputsPath %>% file.path("pop", "pop") %>% paste(768, sep="_") %>% paste0(".csv"), popform= "wide"
#     )
#   )
#   test_results <- 1:length(test_inputs) %>% lapply(function(j){
#     # "Test " %>% paste0(j, ":") %>% print
#     ### Import inputs
#     if(j<4){
#       inputs_j <- FrEDI::import_inputs(
#         gdpfile  = inputsPath %>% file.path("gdp", "gdp") %>% paste(1, sep="_") %>% paste0(".csv"),
#         popfile  = inputsPath %>% file.path("pop", "pop") %>% paste(1, sep="_") %>% paste0(".csv"), popform= "wide"
#       )
#       inputs_j <- test_inputs[[j]] %>% c(inputs_j)
#     } else{
#       inputs_j <- test_inputs[[j]]
#     }
#     ### Run model
#     test_j      <- FrEDI::run_fredi(
#       inputsList = inputs_j,
#       sectorList = test_sectors,
#       aggLevels  = c("impactyear", "national", "modelaverage"),
#       thru2300   = TRUE,
#       elasticity = 1) %>%
#       mutate(trial = "test")
#     ### Return
#     return(test_j)
#   })
#   # time0.2 <- Sys.time(); time0.2 - time0.1
#   ### Number of non-missing rows should be > 0
#   test_results[[1]] %>% filter(driverUnit=="cm") %>% filter(year>2100) %>% filter(!is.na(annual_impacts)) %>% nrow
#   ### Upper range should be > 0 (shouldn't need to add an "na.rm=TRUE" to `range`...note if you do have to add it)
#   test_results[[4]] %>% filter(model == "GISS-E2-R", driverValue > 3) %>% select(annual_impacts) %>% as.vector %>% range()
#   test_results[[4]] %>% filter(model == "GISS-E2-R") %>% filter(!is.infinite(annual_impacts)) %>% select(annual_impacts) %>% as.vector %>% range(na.rm = TRUE)
#   ### Third 
#   # p_test <- test_results[[1]] %>% filter(sector=="Coastal Properties", driverUnit=="cm", region=="Northeast") %>% ggplot() + geom_line(aes(x=year, y=annual_impacts, color=variant)) 
#   
#   ### Range should probably be > 0 (shouldn't need to add an "na.rm=TRUE" to `range`...note if you do have to add it)
#   test_results[[2]] %>% filter(!is.infinite(annual_impacts)) %>% select(annual_impacts) %>% as.vector %>% range()
#   test_results[[2]] %>% filter(!is.infinite(annual_impacts)) %>% select(annual_impacts) %>% as.vector %>% range(na.rm =TRUE)
# } else{test_results <- NULL}
# ### Check results
# if(!is.null(test_results)){
#   test_results3 <- test_results[[4]] %>% 
#     filter(sector=="Coastal Properties", region=="National Total") %>% 
#     filter(variant=="No Adaptation")
#   p_test <- list(
#     slr = test_results3 %>% ggplot() + geom_line(aes(x=year, y=driverValue, color=region)) +
#       scale_y_continuous("SLR (cm)", limits=c(0, 300)) + scale_x_continuous("Year") +
#       theme(legend.position = "none"),
#     pop = test_results3 %>% ggplot() + geom_line(aes(x=year, y=national_pop/1e6, color=region)) +
#       scale_y_continuous("U.S. Population (Millions)", limits=c(0, 450)) + scale_x_continuous("Year") +
#       theme(legend.position = "none"),
#     gdp = test_results3 %>% ggplot() + geom_line(aes(x=year, y=gdp_usd/1e12, color=region)) +
#       scale_y_continuous("U.S. GDP (Trillions, 2015$)", limits=c(0, 450)) + scale_x_continuous("Year") +
#       theme(legend.position = "none"),
#     impacts = test_results3 %>% ggplot() + geom_line(aes(x=year, y=annual_impacts/1e9, color=region)) +
#       scale_y_continuous("U.S. Annual Impacts (Billions), 2015$", limits=c(0, 1e3)) + scale_x_continuous("Year") +
#       theme(legend.position = "none")
#   )
#   p_test
# }

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
        #try_install_i <- ### Option for a different Git repo
          #withr::with_libpaths(
          #  new = .libPaths()[1],
            #remotes::install_github(
            #  repo   = "https://github.com/USEPA/FrEDI",
            #  subdir = package_i,
            #  type   = "source",
            #  repos  = getOption("repos"),
            #  force  = TRUE,
            #  ref = fredi_branch
            #)
          #) %>% 
          #try
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
# new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
# if(length(new.packages)) install.packages(new.packages, repos = "http://cran.rstudio.com/")
# lapply(list.of.packages, library, character.only = TRUE)

###### Custom Functions ######
## function for the opposite of %in%
"%ni%" <- Negate("%in%")

###### Set Selections ######
c_maxYear        <- 2300
c_selectYears    <- seq(2010, c_maxYear, 1)
c_selectModels   <- c("Average", "Interpolation")
c_aggLevels      <- c("impactyear", "national", "modelaverage")
c_dropCols       <- c("driverValue", "driverUnit", "driverType", "sectorprimary", "includeaggregate")
c_joinCols       <- c("sector", "variant", "impactYear", "impactType", "region", "model_type", "model", "year", "physicalmeasure", "gdp_usd", "national_pop", "gdp_percap", "reg_pop")

###### Set Runs ######
c_runs          <- 1:1e4
#c_runs <- 9520

### Get a random sample of 1000 runs
### Sample of 10 takes 3.7 minutes (~37 seconds/run)
# c_runs1   <- sample(x=c_runs, size=n_samples); c_runs1 %>% head
# c_runs0   <- c(6489, 4343,  623, 3036, 2025, 7956, 9984, 4770, 6319, 2520)
c_runs0 <- outputsPath %>%
  file.path("damages") %>%
  list.files(pattern = "\\.parquet") %>%
  (function(x){sub("\\.parquet", "", x)}) %>%
  (function(x){sub("damages_", "", x)}) %>%
  as.numeric %>%
  sort; 
c_runs0 <- outputsPath
c_runs0 %>% length
# c_runs0 <- (outputsPath %>% file.path("trials.csv") %>% read.csv)$trial %>% sort
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
# ### Other runs
# c_runs2   <- c_runs[!(c_runs %in% c_runs1)]; c_runs2 %>% length

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
### Took about 3.31 hours

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

### Updating parquet files for FrEDI 3.3.0. Batch #1

### END OF SCRIPT. Have a great day!
