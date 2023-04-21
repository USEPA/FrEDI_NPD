#####
## Title:   1_build_parts_for_fredi_2300.R
## Purpose: This file is meant to build complete national gpd (in $2015) 
##          and national and regional population timeseries (2010-2300) from RFF 
##          scenarios, for input into FrEDI. 
##          - RFF sps population is in thousands and is converted to # of people.
##          - RFF sps is in 2011$ and is converted to 2015$
##          - Baseline RFF sps global temperatures are read in and adjusted to reflect 
##            global temperature changes relative to the 1986-2005 average (for input into FrEDI)
##          - Perturbed (1Gt C pulse in 2020) RFF sps global temperatures are read in and 
##            adjusted to reflect global temperature changes relative to the 1986-2005 average (for input into FrEDI)         
## Inputs:  data/external/rffsp_usa.csv
##          data/external/rffsp_fair_sequence.csv
##          data/external/temperature/temperature-CO2-RFF-2020-n10000/results/model_1/TempNorm_1850to1900_global_temperature_norm.parquet
## Outputs: data/input_files/us_regional_population.parquet
##          data/input_files/pop/pop_[scenario].csv
##          data/input_files/us_gross_domestic_product.parquet
##          data/input_files/gdp/gdp_[scenario].csv
##          data/input_files/global_mean_surface_temperature_baseline.parquet
##          data/input_files/temp_baseline/temp_baseline_[scenario].csv
##          data/input_files/global_mean_surface_temperature_perturbed_co2_2020.csv
##          data/input_files/temp_perturbed/temp_perturbed_[scenario].csv
## Written by: US EPA, National Center for Environmental Economics; January 2022
## Last updated: 9/28/2022 by E. McDuffie
#####


##########################
#################  library
##########################

## Clear worksace
rm(list = ls())
gc()

## This function will check if a package is installed and, if not, install it
list.of.packages <- c('magrittr','tidyverse',
                      'readxl', 'arrow',
                      'foreach','doParallel')
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages, repos = "http://cran.rstudio.com/")
lapply(list.of.packages, library, character.only = TRUE)

## turn off scientific notation
options(scipen = 9999999)

##########################
##############  data paths
##########################

external_path = file.path('input/external')
fredi_input_path = file.path('input/input_files')

##########################
####################  data
##########################

## read rffsps for us
rffsp =
  external_path %>% 
  file.path('rffsp_usa.csv') %>%
  read_csv %>%
  rename(gdp_usd = gdp)

## final sample randomly selected in MimiGIVE
rffsp_sample = 
  external_path %>%
  file.path('rffsp_fair_sequence.csv') %>%
  read_csv %>% 
  select(-fair.id)

##########################
##########  set up cluster
##########################

## parallel filter and writing of feather files
parallel::detectCores()
n.cores = parallel::detectCores() - 1
#n.cores <- 1

## make cluster
my.cluster =
  parallel::makeCluster(
    n.cores, 
    type = "PSOCK"
  )

## register it to be used by %dopar%
doParallel::registerDoParallel(cl = my.cluster)

# ## check if it is registered (optional)
# foreach::getDoParRegistered()
# 
# ## how many workers are available? (optional)
# foreach::getDoParWorkers()

###### Iteration List ######
jList <- 1:1e4
#jList <- 9520
#jList <- 9883

##########################
##############  population
##########################
pop = 
  rffsp %>% 
  select(rffsp.id, year, pop) %>% 
  right_join(rffsp_sample) %>% 
  arrange(trial,year) %>% 
  filter(year %in% seq(2020, 2300, 5)) %>% 
  mutate(pop = pop*1e3)

## fredi default scenarios population
# fredi_scenario_pop = read_excel('data/external/20210923_ciraTempBinData.xlsx', sheet = 'defaultScenario', range = 'C3:K20')
fredi_scenario_pop = 
  external_path %>%
  file.path('20220701_FrEDI_config.xlsx') %>%
  read_excel(sheet = 'defaultScenario', range = 'C3:K20')

## add 2010 and 2015 fredi populations to rff-sps 
pop = 
  bind_rows(pop,
            fredi_scenario_pop %>% filter(year %in% c(2010, 2015)) %>% 
              summarise(pop = rowSums(select(., starts_with("pop_")))) %>% 
              mutate(year = c(2010, 2015)) %>% 
              crossing(rffsp_sample)) %>% 
  arrange(trial, year)    

## get regional proportions
pop_proportions = 
  fredi_scenario_pop %>% 
  mutate(total = rowSums(select(., starts_with("pop_"))),
         Northern.Plains.prop = pop_Northern.Plains/total,
         Southern.Plains.prop = pop_Southern.Plains/total,
         Midwest.prop         = pop_Midwest/total,
         Northeast.prop       = pop_Northeast/total,
         Northwest.prop       = pop_Northwest/total,
         Southeast.prop       = pop_Southeast/total,
         Southwest.prop       = pop_Southwest/total) %>% 
  select(year, contains('.prop')) %>% 
  complete(year = seq(min(year), 2300, 5)) %>% 
  fill(-year)

## create regional pop from rffsps
pop_regional = 
  pop %>% 
  left_join(pop_proportions) %>% 
  mutate(`Northern Plains` = pop * Northern.Plains.prop,
         `Southern Plains` = pop * Southern.Plains.prop,
         Midwest           = pop * Midwest.prop,
         Northeast         = pop * Northeast.prop,
         Northwest         = pop * Northwest.prop,
         Southeast         = pop * Southeast.prop,
         Southwest         = pop * Southwest.prop) %>% 
  select(trial, rffsp.id, year, `Northern Plains`, `Southern Plains`, Midwest, Northeast, Northwest, Southeast, Southwest)

## export as inputs to fredi
pop_regional %>%
  write_parquet(fredi_input_path %>% file.path('us_regional_population.parquet'))

## export scenario-specific population files to read into fredi
foreach(j = jList, .packages=c('tidyverse')) %dopar% {
  pop_regional %>% 
    filter(trial==j) %>% 
    select(-trial, -rffsp.id) %>% 
    write_csv(fredi_input_path %>% file.path('pop',paste0('pop_', j, '.csv')))
}

##########################
#####################  gdp
##########################

## rff-sps are in 2011$, inflate to 2015 USD; recovered 1/13/2022: https://apps.bea.gov/iTable/iTable.cfm?reqid=19&step=3&isuri=1&select_all_years=0&nipa_table_list=13&series=a&first_year=2011&last_year=2015&scale=-99&categories=survey&thetable=
pricelevel_2011_to_2015 = 104.691/98.164

## fredi default scenarios population
fredi_scenario_gdp = 
  external_path %>%
  file.path('20220701_FrEDI_config.xlsx') %>%
  read_excel(sheet = 'defaultScenario', range = 'C3:D20')

## compile gdp data
gdp = 
  rffsp %>% 
  select(rffsp.id, year, gdp_usd, dollar.year) %>% 
  right_join(rffsp_sample) %>% 
  arrange(trial, year) %>% 
  filter(year %in% seq(2010, 2300, 5)) %>% 
  mutate(gdp_usd     = gdp_usd * pricelevel_2011_to_2015 * 1e6,
         dollar.year = 2015) %>% 
  relocate(trial, rffsp.id, year)

## add 2010 and 2015 fredi populations to rff-sps 
gdp = 
  bind_rows(gdp,
            fredi_scenario_gdp %>% 
              filter(year %in% c(2010, 2015)) %>% 
              mutate(dollar.year = 2015) %>% 
              crossing(rffsp_sample)) %>% 
  arrange(trial, year)

## export as inputs to fredi
gdp %>% 
  write_parquet(fredi_input_path %>% file.path('us_gross_domestic_product.parquet'))

## export scenario-specific files to read into fredi
foreach(j = jList, .packages=c('tidyverse')) %dopar% {
  gdp %>% 
    filter(trial==j) %>% 
    select(year, gdp_usd) %>% 
    write_csv(fredi_input_path %>% file.path('gdp',paste0('gdp_', j, '.csv')))
}

##########################
#############  temperature
##########################

## the following processing script can be applied to any baseline and perturbed temperature output from MimiGIVE to prepare it for FrEDI

## baseline temperature 
temp = 
  external_path %>%
  file.path('temperature','temperature-CO2-RFF-2020-n10000','results','model_1','TempNorm_1850to1900_global_temperature_norm.parquet') %>%
  read_parquet %>% 
  rename(year=time, temp_C_global=2, trial=trialnum) %>% 
  filter(year > 1985)

## recover 1986 to 2005 mean to scale for fredi
temp.relative.baseline = 
  temp %>% 
  filter(year %in% seq(1986, 2005, 1)) %>% 
  group_by(trial) %>% 
  summarise(base.1986.2005 = mean(temp_C_global)) %>% 
  ungroup()

## rescale temp and trim for fredi
temp %<>% left_join(temp.relative.baseline) %>% 
  mutate(temp_C_global = temp_C_global - base.1986.2005) %>% 
  filter(year %in% seq(2000, 2300, 1)) %>%
  select(-base.1986.2005) %>% 
  left_join(rffsp_sample) %>% 
  relocate(trial, rffsp.id, year)

## export as inputs to fredi
temp %>% 
  write_parquet(fredi_input_path %>% file.path('global_mean_surface_temperature_baseline.parquet'))

## export scenario-specific files to read into fredi
foreach(j = jList, .packages=c('tidyverse')) %dopar% {
  temp %>% 
    filter(trial==j) %>% 
    select(year, temp_C_global) %>% 
    write_csv(fredi_input_path %>% file.path('temp_baseline', paste0('temp_baseline_', j, '.csv')))
}

## repeat with each temperature path from the perturbed emissions for each gas
for (GAS in c('co2', 'ch4', 'n2o')) {
  for (YEAR in c(2020))
    
    ## perturbed temperature path
    temp =
      external_path %>%
      file.path('temperature', paste0('temperature-', toupper(GAS), '-RFF-', YEAR, '-n10000'),
                'results', 'model_2', 'TempNorm_1850to1900_global_temperature_norm.parquet') %>%
      read_parquet %>% 
      rename(year          = time, 
             temp_C_global = 2, 
             trial         = trialnum) %>% 
      filter(year > 1985)
  
  ## rescale temp and trim for fredi
  temp %<>% 
    left_join(temp.relative.baseline) %>% 
    mutate(temp_C_global = temp_C_global - base.1986.2005) %>% 
    filter(year %in% seq(2000, 2300, 1)) %>%
    select(-base.1986.2005) %>% 
    left_join(rffsp_sample) %>% 
    relocate(trial, rffsp.id, year)
  
  ## export as inputs to fredi
  temp %>% 
    write_parquet(fredi_input_path %>% 
                           file.path(paste0('global_mean_surface_temperature_perturbed_', GAS, '_', YEAR, '.parquet')))
  
  ## export scenario-specific files to read into fredi
  foreach(j = jList, .packages=c('tidyverse')) %dopar% {
    temp %>% 
      filter(trial == j) %>% 
      select(year, temp_C_global) %>% 
      write_csv(fredi_input_path %>% 
                  file.path('temp_perturbed', paste0(GAS, '/temp_perturbed_', GAS, '_', YEAR, '_', j, '.csv')))
  }
}

## stop cluster
parallel::stopCluster(cl = my.cluster)

## end of script, have a great day!