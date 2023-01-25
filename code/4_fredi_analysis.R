#####
## Title:   5_fredi_analysis.R
## Purpose: This file will read all FrEDI output files and generate aggregate dataframes
##          for use in the scc graphics script.
##          
## Inputs:  output/damages/rffsp/damages_[scenario].parquet
## Outputs: output/damages/rffsp/damages_[scenario].parquet
## Written by: US EPA, Climate Change Division (OAP); November 2022
### Edited 2022.11.29
## Last updated: 11/29/2022 by E. McDuffie
## Notes:       
##
#####

###### Clear Workspace ######
rm(list = ls()); gc()

###### Load packages ###### 
require(tidyverse);
require(pbapply);
require(arrow);
require(magrittr);
require(dplyr);


###### Set Paths ######
#eem (b/c I don't haver permission to open SCC_FrEDI.Rproj)
setwd("/home/emcduf01/shared/ncee/scghg/FrEDI/SCC_FrEDI/")
inputsPath   <- "." %>% file.path("output", "damages","rffsp")
outputsPath  <- "." %>% file.path("output", "damages","fredi_analysis")
#original scghg
#scghgPath    <- "." %>% file.path("output","scghg")
##transformed scghg path (updated 1/5/23)
scghgPath    <- "." %>% file.path("output","scghg_damage_transformation")

##### Collect Damage Files ####
c_iteration  <- inputsPath %>% file.path("damages") %>% list.files(pattern = "\\.parquet") %>%
  (function(x){sub("\\.parquet", "", x)}) %>%
  (function(x){sub("damages_", "", x)}) %>%
  as.numeric %>% sort; 
c_iteration %>% length
#c_iteration <- c_iteration[1:10]

excluded_sectors <- c('Extreme Temperature','CIL Extreme Temperature','Asphalt Roads')
c_select_rawCols <- c("sector", "variant", "impactType", "region", "year", "model_type", 
  "sectorprimary", "physicalmeasure", "trial", "damageType","driverType","driverValue","reg_pop","national_pop","gdp_usd")

#### Set Progress Bar ####
op <- pboptions(type = "timer",char = "=")



###### Read-in/Load Data ######

###### National Baseline Data ######
##1. Read in all Baseline National Damages (for primary sectors/variants) from all trials
    #can remove CIL Extreme Temp, Asphalt Roads, and Extreme Temp sectors
# If df is too large...
  # 1a. read in baseline damages, chunked into smaller sets of years

  #outputs
  #1b. df of baseline statistics for each sector across all trials (summed across impact types) (?)
  #1a. df of baseline damages for each sector and each trial (summed across impact types) 


# 1Aa - baseline damages from all impact types for 2000-2050 (all years, all trials)

# format from the raw data if the data file does not yet exist (or if reload ==0)
load_raw = 1
reload <- ifelse(file.exists(outputsPath %>% file.path("baseline_impacts_nat_thru2050_constrained.parquet")) 
                 & load_raw ==0,1,0)

if (reload ==1){
  df_fraw_allnat <- read_parquet(outputsPath %>% file.path("baseline_impacts_nat_thru2050_constrained.parquet"))
  
}else{
  df_fraw_allnat <- 
    pblapply(1:length(c_iteration), function(i){
      ### File name
      infile_i  <- inputsPath %>%
        file.path("damages", "damages") %>%
        paste(c_iteration[i], sep="_") %>%
        paste0(".", "parquet")
      ### Read in data and return
      data_i    <- infile_i %>% read_parquet
      ### Filter data for model type, national total, desired sectors, baseline scenario
      data_i    <- data_i   %>% 
        filter(model %in% c("Average", "Interpolation")) %>% #filters model type
        filter(year <= 2050) %>%                             #filter for first part of time series (to minimize df size)
        filter(region =='National Total') %>%                #filter for national region
        filter(sectorprimary==1) %>%                         #filters for primary variant
        filter(!sector %in% excluded_sectors) %>%            #removes sectors not needed
        filter(damageType =='Baseline') %>%                  #selects only baseline case
        # sum across impact type and physical measure (e.g., sums different physical impact types)
        group_by_at(.vars = c_select_rawCols[!(c_select_rawCols %in% c("impactType","physicalmeasure"))]) %>%
        summarize_at(.vars = c("annual_impacts"), sum, na.rm = TRUE) %>%
        ungroup %>%
        select(-c('model_type','sectorprimary','variant','region','driverType','driverValue','damageType','national_pop','reg_pop'))
        
        #Could sum across sectors (group by year and trial), then use that total damage value for each year for the transformation (with gdp), 
        #then scale all sectors the GDP dependence in data_i damages, then return data_i
      
      #Correct damages to account for the fact that GDP is impacted by climate damages, thus corrected damages and final gdp will be lower  
      ####################
      ### NOTE (1/5/23): this simplified version corrects damages from all sectors, even those that are not dependent on GDP
      ### In future versions, a different multiplier equation should be implemented to capture the actual dependence of each 
      ###  individual sector on GDP and only correct those sectors that have an explict GD dependence. 
      ###################
      #1. Calculate initial total damages (group by year and trial), D0
      D0 <- data_i %>%
        group_by_at(.vars = c("year","trial","gdp_usd")) %>%
        summarize_at(.vars = c("annual_impacts"), sum, na_rm = TRUE)
      
      #2. calculate scalar using total damage and GDP.  
          #Df = D0/(1+D0/GDP0)
          #multiplier = 1/(1+D0/GDP0)
      D0 <- D0 %>%
            mutate(multiplier = 1/(1+(annual_impacts/gdp_usd)))
      
      #3. then scale all sectors the GDP dependence in data_i damages, ###NOTE -- 
        #first join the annual multiplier with the original data, second, scale all sectors
      data_i_scaled <- left_join(data_i, D0 %>% select(multiplier,year,trial), by = c('year','trial'))
      data_i_scaled <- data_i_scaled %>%
        mutate(annual_impacts_scaled = annual_impacts *multiplier)%>%
        mutate(annual_impacts = annual_impacts_scaled) %>%
        select(c("sector","year","trial","annual_impacts","multiplier"))#,"annual_impacts_scaled"))
        
      #4. then return corrected data
      ### Return data
      return(data_i_scaled)
    }) %>%
    
    ### Bind the data together
    (function(x){
      do.call(rbind, x)
    }); df_fraw_allnat %>% glimpse
  ### Save file
  df_fraw_allnat %>%
    write_parquet(outputsPath %>% file.path("baseline_impacts_nat_thru2050_constrained.parquet"))
} 

# 1Ba - baseline statistics for each year and sector across all trials (summed across impact types) 
# Table 1, Table A1, Figure 2, Figure 3 , Figure A2, A3
df_fstat <- df_fraw_allnat %>%
  group_by_at(.vars = c('sector','year')) %>%
  summarize(X025=quantile(annual_impacts,probs=0.025), 
            X50=quantile(annual_impacts, probs=0.50),
            X975=quantile(annual_impacts,probs=0.975),
            X005=quantile(annual_impacts,probs=0.005),
            X995=quantile(annual_impacts,probs=0.995),
            mean=mean(annual_impacts)) %>% ungroup

df_fstat %>%
  write_parquet(outputsPath %>% file.path("baseline_impacts_nat_thru2050_stats_constrained.parquet"))


# 1Ab - baseline damages from 2050-2100 (all years, all trials)

# format from the raw data if the data file does not yet exist (or if reload ==0)
load_raw = 1
reload <- ifelse(file.exists(outputsPath %>% file.path("baseline_impacts_nat_2050-2100_constrained.parquet")) 
                 & load_raw ==0,1,0)

if (reload ==1){
  df_fraw_allnat <- read_parquet(outputsPath %>% file.path("baseline_impacts_nat_2050-2100_constrained.parquet"))

}else{
  df_fraw_allnat <- 
    pblapply(1:length(c_iteration), function(i){
      ### File name
      infile_i  <- inputsPath %>%
        file.path("damages", "damages") %>%
        paste(c_iteration[i], sep="_") %>%
        paste0(".", "parquet")
      ### Read in data and return
      data_i    <- infile_i %>% read_parquet
      ### Filter data for model type, national total, desired sectors, baseline scenario
      data_i    <- data_i   %>% 
        filter(model %in% c("Average", "Interpolation")) %>% #filters model type
        filter(year > 2050 & year <= 2100) %>%                             #filter for first part of time series (to minimize df size)
        filter(region =='National Total') %>%                #filter for national region
        filter(sectorprimary==1) %>%                         #filters for primary variant
        filter(!sector %in% excluded_sectors) %>%            #removes sectors not needed
        filter(damageType =='Baseline') %>%                  #selects only baseline case
        # sum across impact type and physical measure (e.g., sums different physical impact types)
        group_by_at(.vars = c_select_rawCols[!(c_select_rawCols %in% c("impactType","physicalmeasure"))]) %>%
        summarize_at(.vars = c("annual_impacts"), sum, na.rm = TRUE) %>%
        ungroup %>%
        select(-c('model_type','sectorprimary','variant','region','driverType','driverValue','damageType',"reg_pop","national_pop")) 
      #constrain damages  
      D0 <- data_i %>%
        group_by_at(.vars = c("year","trial","gdp_usd")) %>%
        summarize_at(.vars = c("annual_impacts"), sum, na_rm = TRUE)
      D0 <- D0 %>%
        mutate(multiplier = 1/(1+(annual_impacts/gdp_usd)))
      data_i_scaled <- left_join(data_i, D0 %>% select(multiplier,year,trial), by = c('year','trial'))
      data_i_scaled <- data_i_scaled %>%
        mutate(annual_impacts_scaled = annual_impacts *multiplier)%>%
        mutate(annual_impacts = annual_impacts_scaled) %>%
        select(c("sector","year","trial","annual_impacts","multiplier"))#,"annual_impacts_scaled"))
      ### Return data
      return(data_i_scaled)
    }) %>%
    ### Bind the data together
    (function(x){
      do.call(rbind, x)
    }); df_fraw_allnat %>% glimpse
  ### Save file
  df_fraw_allnat %>%
    write_parquet(outputsPath %>% file.path("baseline_impacts_nat_2050-2100_constrained.parquet"))
} 

# 1Bb - baseline statistics for each year and sector across all trials (summed across impact types) 
# Table 1, Table A1, Figure 2, Figure 3 , Figure A2, A3
df_fstat <- df_fraw_allnat %>%
  group_by_at(.vars = c('sector','year')) %>%
  summarize(X025=quantile(annual_impacts,probs=0.025), 
            X50=quantile(annual_impacts, probs=0.50),
            X975=quantile(annual_impacts,probs=0.975),
            X005=quantile(annual_impacts,probs=0.005),
            X995=quantile(annual_impacts,probs=0.995),
            mean=mean(annual_impacts)) %>% ungroup

df_fstat %>%
  write_parquet(outputsPath %>% file.path("baseline_impacts_nat_2050-2100_stats_constrained.parquet"))

#***
## 1Ac - baseline damages from all impact types (2100-2150, all trials)

# format from the raw data if the data file does not yet exist (or if reload ==0)
load_raw = 1
reload <- ifelse(file.exists(outputsPath %>% file.path("baseline_impacts_nat_2100-2150_constrained.parquet")) 
                 & load_raw ==0,1,0)

if (reload ==1){
  df_fraw_allnat <- read_parquet(outputsPath %>% file.path("baseline_impacts_nat_2100-2150_constrained.parquet"))
}else{
  df_fraw_allnat <- 
    pblapply(1:length(c_iteration), function(i){
      ### File name
      infile_i  <- inputsPath %>%
        file.path("damages", "damages") %>%
        paste(c_iteration[i], sep="_") %>%
        paste0(".", "parquet")
      ### Read in data and return
      data_i    <- infile_i %>% read_parquet
      ### Filter data for model type, national total, desired sectors, baseline scenario
      data_i    <- data_i   %>% 
        filter(model %in% c("Average", "Interpolation")) %>% #filters model type
        filter(year > 2100 & year <= 2150) %>%               #filter for first part of time series (to minimize df size)
        filter(region =='National Total') %>%                #filter for national region
        filter(sectorprimary==1) %>%                         #filters for primary variant
        filter(!sector %in% excluded_sectors) %>%            #removes sectors not needed
        filter(damageType =='Baseline') %>%                  #selects only baseline case
        # sum across impact type and physical measure (e.g., sums different physical impact types)
        group_by_at(.vars = c_select_rawCols[!(c_select_rawCols %in% c("impactType","physicalmeasure"))]) %>%
        summarize_at(.vars = c("annual_impacts"), sum, na.rm = TRUE) %>%
        ungroup %>%
        select(-c('model_type','sectorprimary','variant','region','driverType','driverValue','damageType','reg_pop','national_pop'))
      #constrain damages  
      D0 <- data_i %>%
        group_by_at(.vars = c("year","trial","gdp_usd")) %>%
        summarize_at(.vars = c("annual_impacts"), sum, na_rm = TRUE)
      D0 <- D0 %>%
        mutate(multiplier = 1/(1+(annual_impacts/gdp_usd)))
      data_i_scaled <- left_join(data_i, D0 %>% select(multiplier,year,trial), by = c('year','trial'))
      data_i_scaled <- data_i_scaled %>%
        mutate(annual_impacts_scaled = annual_impacts *multiplier)%>%
        mutate(annual_impacts = annual_impacts_scaled) %>%
        select(c("sector","year","trial","annual_impacts","multiplier"))#,"annual_impacts_scaled"))
      ### Return data
      return(data_i_scaled)
    }) %>%
    ### Bind the data together
    (function(x){
      do.call(rbind, x)
    }); df_fraw_allnat %>% glimpse
  ### Save file
  df_fraw_allnat %>%
    write_parquet(outputsPath %>% file.path("baseline_impacts_nat_2100-2150_constrained.parquet"))
} 

# 1Bc - baseline statistics for each year and sector across all trials (summed across impact types) 
# Table 1, Table A1, Figure 2, Figure 3 , Figure A2, A3
df_fstat <- df_fraw_allnat %>%
  group_by_at(.vars = c('sector','year')) %>%
  summarize(X025=quantile(annual_impacts,probs=0.025), 
            X50=quantile(annual_impacts, probs=0.50),
            X975=quantile(annual_impacts,probs=0.975),
            X005=quantile(annual_impacts,probs=0.005),
            X995=quantile(annual_impacts,probs=0.995),
            mean=mean(annual_impacts)) %>% ungroup

df_fstat %>%
  write_parquet(outputsPath %>% file.path("baseline_impacts_nat_2100-2150_stats_constrained.parquet"))


#***
## 1Ad - baseline damages from all impact types (2150-2200, all trials)

# format from the raw data if the data file does not yet exist (or if reload ==0)
load_raw = 1
reload <- ifelse(file.exists(outputsPath %>% file.path("baseline_impacts_nat_2150-2200_constrained.parquet")) 
                 & load_raw ==0,1,0)

if (reload ==1){
  df_fraw_allnat <- read_parquet(outputsPath %>% file.path("baseline_impacts_nat_2150-2200_constrained.parquet"))
}else{
  df_fraw_allnat <- 
    pblapply(1:length(c_iteration), function(i){
      ### File name
      infile_i  <- inputsPath %>%
        file.path("damages", "damages") %>%
        paste(c_iteration[i], sep="_") %>%
        paste0(".", "parquet")
      ### Read in data and return
      data_i    <- infile_i %>% read_parquet
      ### Filter data for model type, national total, desired sectors, baseline scenario
      data_i    <- data_i   %>% 
        filter(model %in% c("Average", "Interpolation")) %>% #filters model type
        filter(year > 2150 & year <= 2200) %>%               #filter for first part of time series (to minimize df size)
        filter(region =='National Total') %>%                #filter for national region
        filter(sectorprimary==1) %>%                         #filters for primary variant
        filter(!sector %in% excluded_sectors) %>%            #removes sectors not needed
        filter(damageType =='Baseline') %>%                  #selects only baseline case
        # sum across impact type and physical measure (e.g., sums different physical impact types)
        group_by_at(.vars = c_select_rawCols[!(c_select_rawCols %in% c("impactType","physicalmeasure"))]) %>%
        summarize_at(.vars = c("annual_impacts"), sum, na.rm = TRUE) %>%
        ungroup %>%
        select(-c('model_type','sectorprimary','variant','region','driverType','driverValue','damageType','national_pop','reg_pop')) 
      #constrain damages  
      D0 <- data_i %>%
        group_by_at(.vars = c("year","trial","gdp_usd")) %>%
        summarize_at(.vars = c("annual_impacts"), sum, na_rm = TRUE)
      D0 <- D0 %>%
        mutate(multiplier = 1/(1+(annual_impacts/gdp_usd)))
      data_i_scaled <- left_join(data_i, D0 %>% select(multiplier,year,trial), by = c('year','trial'))
      data_i_scaled <- data_i_scaled %>%
        mutate(annual_impacts_scaled = annual_impacts *multiplier)%>%
        mutate(annual_impacts = annual_impacts_scaled) %>%
        select(c("sector","year","trial","annual_impacts","multiplier"))#,"annual_impacts_scaled"))
      ### Return data
      return(data_i_scaled)
    }) %>%
    ### Bind the data together
    (function(x){
      do.call(rbind, x)
    }); df_fraw_allnat %>% glimpse
  ### Save file
  df_fraw_allnat %>%
    write_parquet(outputsPath %>% file.path("baseline_impacts_nat_2150-2200_constrained.parquet"))
} 

# 1Bd - baseline statistics for each year and sector across all trials (summed across impact types) 
# Table 1, Table A1, Figure 2, Figure 3 , Figure A2, A3
df_fstat <- df_fraw_allnat %>%
  group_by_at(.vars = c('sector','year')) %>%
  summarize(X025=quantile(annual_impacts,probs=0.025), 
            X50=quantile(annual_impacts, probs=0.50),
            X975=quantile(annual_impacts,probs=0.975),
            X005=quantile(annual_impacts,probs=0.005),
            X995=quantile(annual_impacts,probs=0.995),
            mean=mean(annual_impacts)) %>% ungroup

df_fstat %>%
  write_parquet(outputsPath %>% file.path("baseline_impacts_nat_2150-2200_stats_constrained.parquet"))



## 1Ae - baseline damages from all impact types (2200-2250, all trials)

# format from the raw data if the data file does not yet exist (or if reload ==0)
load_raw = 1
reload <- ifelse(file.exists(outputsPath %>% file.path("baseline_impacts_nat_2200-2250_constrained.parquet")) 
                 & load_raw ==0,1,0)

if (reload ==1){
  df_fraw_allnat <- read_parquet(outputsPath %>% file.path("baseline_impacts_nat_2200-2250_constrained.parquet"))
}else{
  df_fraw_allnat <- 
    pblapply(1:length(c_iteration), function(i){
      ### File name
      infile_i  <- inputsPath %>%
        file.path("damages", "damages") %>%
        paste(c_iteration[i], sep="_") %>%
        paste0(".", "parquet")
      ### Read in data and return
      data_i    <- infile_i %>% read_parquet
      ### Filter data for model type, national total, desired sectors, baseline scenario
      data_i    <- data_i   %>% 
        filter(model %in% c("Average", "Interpolation")) %>% #filters model type
        filter(year > 2200 & year <= 2250) %>%               #filter for first part of time series (to minimize df size)
        filter(region =='National Total') %>%                #filter for national region
        filter(sectorprimary==1) %>%                         #filters for primary variant
        filter(!sector %in% excluded_sectors) %>%            #removes sectors not needed
        filter(damageType =='Baseline') %>%                  #selects only baseline case
        # sum across impact type and physical measure (e.g., sums different physical impact types)
        group_by_at(.vars = c_select_rawCols[!(c_select_rawCols %in% c("impactType","physicalmeasure"))]) %>%
        summarize_at(.vars = c("annual_impacts"), sum, na.rm = TRUE) %>%
        ungroup %>%
        select(-c('model_type','sectorprimary','variant','region','driverType','driverValue','damageType','national_pop','reg_pop')) 
      #constrain damages  
      D0 <- data_i %>%
        group_by_at(.vars = c("year","trial","gdp_usd")) %>%
        summarize_at(.vars = c("annual_impacts"), sum, na_rm = TRUE)
      D0 <- D0 %>%
        mutate(multiplier = 1/(1+(annual_impacts/gdp_usd)))
      data_i_scaled <- left_join(data_i, D0 %>% select(multiplier,year,trial), by = c('year','trial'))
      data_i_scaled <- data_i_scaled %>%
        mutate(annual_impacts_scaled = annual_impacts *multiplier)%>%
        mutate(annual_impacts = annual_impacts_scaled) %>%
        select(c("sector","year","trial","annual_impacts","multiplier"))#,"annual_impacts_scaled"))
      ### Return data
      return(data_i_scaled)
    }) %>%
    ### Bind the data together
    (function(x){
      do.call(rbind, x)
    }); df_fraw_allnat %>% glimpse
  ### Save file
  df_fraw_allnat %>%
    write_parquet(outputsPath %>% file.path("baseline_impacts_nat_2200-2250_constrained.parquet"))
} 

# 1Be - baseline statistics for each year and sector across all trials (summed across impact types) 
# Table 1, Table A1, Figure 2, Figure 3 , Figure A2, A3
df_fstat <- df_fraw_allnat %>%
  group_by_at(.vars = c('sector','year')) %>%
  summarize(X025=quantile(annual_impacts,probs=0.025), 
            X50=quantile(annual_impacts, probs=0.50),
            X975=quantile(annual_impacts,probs=0.975),
            X005=quantile(annual_impacts,probs=0.005),
            X995=quantile(annual_impacts,probs=0.995),
            mean=mean(annual_impacts)) %>% ungroup

df_fstat %>%
  write_parquet(outputsPath %>% file.path("baseline_impacts_nat_2200-2250_stats_constrained.parquet"))



#*** 1Af - baseline damages from all impact types (2250-2300, all trials)

# format from the raw data if the data file does not yet exist (or if reload ==0)
load_raw = 1
reload <- ifelse(file.exists(outputsPath %>% file.path("baseline_impacts_nat_2250-2300_constrained.parquet")) 
                 & load_raw ==0,1,0)

if (reload ==1){
  df_fraw_allnat <- read_parquet(outputsPath %>% file.path("baseline_impacts_nat_2250-2300_constrained.parquet"))
}else{
  df_fraw_allnat <- 
    pblapply(1:length(c_iteration), function(i){
      ### File name
      infile_i  <- inputsPath %>%
        file.path("damages", "damages") %>%
        paste(c_iteration[i], sep="_") %>%
        paste0(".", "parquet")
      ### Read in data and return
      data_i    <- infile_i %>% read_parquet
      ### Filter data for model type, national total, desired sectors, baseline scenario
      data_i    <- data_i   %>% 
        filter(model %in% c("Average", "Interpolation")) %>% #filters model type
        filter(year > 2250 & year <= 2300) %>%               #filter for first part of time series (to minimize df size)
        filter(region =='National Total') %>%                #filter for national region
        filter(sectorprimary==1) %>%                         #filters for primary variant
        filter(!sector %in% excluded_sectors) %>%            #removes sectors not needed
        filter(damageType =='Baseline') %>%                  #selects only baseline case
        # sum across impact type and physical measure (e.g., sums different physical impact types)
        group_by_at(.vars = c_select_rawCols[!(c_select_rawCols %in% c("impactType","physicalmeasure"))]) %>%
        summarize_at(.vars = c("annual_impacts"), sum, na.rm = TRUE) %>%
        ungroup %>%
        select(-c('model_type','sectorprimary','variant','region','driverType','driverValue','damageType','national_pop','reg_pop')) 
      #constrain damages  
      D0 <- data_i %>%
        group_by_at(.vars = c("year","trial","gdp_usd")) %>%
        summarize_at(.vars = c("annual_impacts"), sum, na_rm = TRUE)
      D0 <- D0 %>%
        mutate(multiplier = 1/(1+(annual_impacts/gdp_usd)))
      data_i_scaled <- left_join(data_i, D0 %>% select(multiplier,year,trial), by = c('year','trial'))
      data_i_scaled <- data_i_scaled %>%
        mutate(annual_impacts_scaled = annual_impacts *multiplier)%>%
        mutate(annual_impacts = annual_impacts_scaled) %>%
        select(c("sector","year","trial","annual_impacts","multiplier"))#,"annual_impacts_scaled"))
      ### Return data
      return(data_i_scaled)
    }) %>%
    ### Bind the data together
    (function(x){
      do.call(rbind, x)
    }); df_fraw_allnat %>% glimpse
  ### Save file
  df_fraw_allnat %>%
    write_parquet(outputsPath %>% file.path("baseline_impacts_nat_2250-2300_constrained.parquet"))
} 

# 1Bf - baseline statistics for each year and sector across all trials (summed across impact types) 
# Table 1, Table A1, Figure 2, Figure 3 , Figure A2, A3
df_fstat <- df_fraw_allnat %>%
  group_by_at(.vars = c('sector','year')) %>%
  summarize(X025=quantile(annual_impacts,probs=0.025), 
            X50=quantile(annual_impacts, probs=0.50),
            X975=quantile(annual_impacts,probs=0.975),
            X005=quantile(annual_impacts,probs=0.005),
            X995=quantile(annual_impacts,probs=0.995),
            mean=mean(annual_impacts)) %>% ungroup

df_fstat %>%
  write_parquet(outputsPath %>% file.path("baseline_impacts_nat_2250-2300_stats_constrained.parquet"))





###### Regional Baseline Data ######
##2. Read in all Regional Baseline Damages for the year 2090 from all trial

  #outputs
  #2a. df of baseline damages for each sector and region and trial (summed across impact types)
  #2b df of baseline statistics for each sector, across all trials (summed across impact types)

# 2A - baseline damages from all impact types (2090 only, all regions, all trials)
# Figure 5, Figure A3/4

# format from the raw data if the data file does not yet exist (or if reload ==0)
load_raw = 1
reload <- ifelse(file.exists(outputsPath %>% file.path("impacts_reg_2090_constrained.parquet")) 
                 & load_raw ==0,1,0)

if (reload ==1){
  df_fraw_2090reg <- read_parquet(outputsPath %>% file.path("impacts_reg_2090_constrained.parquet"))
}else{
  df_fraw_2090reg <- 
    pblapply(1:length(c_iteration), function(i){
      ### File name
      infile_i  <- inputsPath %>%
        file.path("damages", "damages") %>%
        paste(c_iteration[i], sep="_") %>%
        paste0(".", "parquet")
      ### Read in data and return
      data_i    <- infile_i %>% read_parquet
      ### Filter data for model type, national total, desired sectors, baseline scenario
      data_i    <- data_i   %>% 
        filter(model %in% c("Average", "Interpolation")) %>% #filters model type
        filter(!region =='National Total') %>%               #filter for all regions
        filter(sectorprimary==1) %>%                         #filters for primary variant
        filter(!sector %in% excluded_sectors) %>%            #removes sectors not needed
        filter(damageType =='Baseline') %>%                  #selects only baseline case
        filter(year ==2090) %>%                              #selects 2090 data only
        # sum across impact type and physical measure (e.g., sums different physical impact types)
        group_by_at(.vars = c_select_rawCols[!(c_select_rawCols %in% c("impactType","physicalmeasure"))]) %>%
        summarize_at(.vars = c("annual_impacts"), sum, na.rm = TRUE) %>%
        ungroup %>%
        select(-c('model_type','sectorprimary','variant','driverType','driverValue')) #%>%
      #constrain damages  
      D0 <- data_i %>%
        group_by_at(.vars = c("year","trial","gdp_usd")) %>%
        summarize_at(.vars = c("annual_impacts"), sum, na_rm = TRUE)
      D0 <- D0 %>%
        mutate(multiplier = 1/(1+(annual_impacts/gdp_usd)))
      data_i_scaled <- left_join(data_i, D0 %>% select(multiplier,year,trial), by = c('year','trial'))
      data_i_scaled <- data_i_scaled %>%
        mutate(annual_impacts_scaled = annual_impacts *multiplier)%>%
        mutate(annual_impacts = annual_impacts_scaled) %>%
        select(c("sector","region","year","trial","damageType","reg_pop","annual_impacts","multiplier"))#,"annual_impacts_scaled"))
      ### Return data
      return(data_i_scaled)
    }) %>%
    ### Bind the data together
    (function(x){
      do.call(rbind, x)
    }); df_fraw_2090reg %>% glimpse
  ### Save file
  df_fraw_2090reg %>%
    write_parquet(outputsPath %>% file.path("impacts_reg_2090_constrained.parquet"))
} 

# 2B - baseline statistics for each year and sector across all trials (summed across impact types) 
# 
df_fstat_2090reg <- df_fraw_2090reg %>%
  group_by_at(.vars = c('sector','year','region','damageType')) %>%
  summarize(X025=quantile(annual_impacts,probs=0.025), 
            X50=quantile(annual_impacts, probs=0.50),
            X975=quantile(annual_impacts,probs=0.975),
            X005=quantile(annual_impacts,probs=0.005),
            X995=quantile(annual_impacts,probs=0.995),
            mean=mean(annual_impacts)) %>% ungroup

df_fstat_2090reg %>%
  write_parquet(outputsPath %>% file.path("impacts_reg_2090_stats_constrained.parquet"))


###### Adaptation Options ######
##3. Adapation - read in all baseline national damages for sectors with adapation only, for the year 2090 (all trials)

  #outputs:
  #3a. df of baseline damages for tehse select sectors

# format from the raw data if the data file does not yet exist (or if reload ==0)
load_raw = 1
reload <- ifelse(file.exists(outputsPath %>% file.path("impacts_adapt_2090_constrained.parquet")) 
                 & load_raw ==0,1,0)

if (reload ==1){
  df_fraw_2090adapt <- read_parquet(outputsPath %>% file.path("impacts_adapt_2090_constrained.parquet"))
  #iter_count <- iter_count +length(c_iteration)
}else{
  df_fraw_2090adapt <- 
    pblapply(1:length(c_iteration), function(i){
      ### File name
      infile_i  <- inputsPath %>%
        file.path("damages", "damages") %>%
        paste(c_iteration[i], sep="_") %>%
        paste0(".", "parquet")
      ### Read in data and return
      data_i    <- infile_i %>% read_parquet
      ### Filter data for model type, national total, desired sectors, baseline scenario
      data_i    <- data_i   %>% 
        filter(model %in% c("Average", "Interpolation")) %>% #filters model type
        filter(region =='National Total') %>%               #filter for all regions
        #filter(sectorprimary==1) %>%                         #filters for primary variant
        filter(!sector %in% excluded_sectors) %>%            #removes sectors not needed
        filter(damageType =='Baseline') %>%                  #selects only baseline case
        filter(year ==2090) %>%                              #selects 2090 data only
        #filter(sector %in% c('Electricity Transmission and Distribution',
        #                     'Rail','Roads','Coastal Properties',
        #                     'High Tide Flooding and Traffic')) %>% #select only sectors with adaptation options
        # sum across impact type and physical measure (e.g., sums different physical impact types)
        group_by_at(.vars = c_select_rawCols[!(c_select_rawCols %in% c("impactType","physicalmeasure"))]) %>%
        summarize_at(.vars = c("annual_impacts"), sum, na.rm = TRUE) %>%
        ungroup %>%
        select(-c('model_type','driverType','driverValue')) 
      
      #constrain damages  
      D0_primary <- data_i %>%
        filter(sectorprimary==1) %>%
        group_by_at(.vars = c("year","trial","gdp_usd")) %>%
        summarize_at(.vars = c("annual_impacts"), sum, na_rm = TRUE) %>%
        ungroup()%>%
        select("annual_impacts")
      
      D0_elec <- data_i %>%
        filter(sector %in% 'Electricity Transmission and Distribution') %>%
        mutate(default_diff = annual_impacts - annual_impacts[which(sectorprimary==1)]) %>%
        mutate(multiplier = 1/(1+((D0_primary$annual_impacts+default_diff)/gdp_usd))) %>%
        mutate(annual_impacts_scaled = annual_impacts *multiplier)%>%
        mutate(annual_impacts = annual_impacts_scaled)
      
      D0_rail <- data_i %>%
        filter(sector %in% 'Rail') %>%
        mutate(default_diff = annual_impacts - annual_impacts[which(sectorprimary==1)]) %>%
        mutate(multiplier = 1/(1+((D0_primary$annual_impacts+default_diff)/gdp_usd))) %>%
        mutate(annual_impacts_scaled = annual_impacts *multiplier)%>%
        mutate(annual_impacts = annual_impacts_scaled)
      
      D0_roads<- data_i %>%
        filter(sector %in% 'Roads') %>%
        mutate(default_diff = annual_impacts - annual_impacts[which(sectorprimary==1)]) %>%
        mutate(multiplier = 1/(1+((D0_primary$annual_impacts+default_diff)/gdp_usd))) %>%
        mutate(annual_impacts_scaled = annual_impacts *multiplier)%>%
        mutate(annual_impacts = annual_impacts_scaled)
      
      D0_coastal <- data_i %>%
        filter(sector %in% 'Coastal Properties') %>%
        mutate(default_diff = annual_impacts - annual_impacts[which(sectorprimary==1)]) %>%
        mutate(multiplier = 1/(1+((D0_primary$annual_impacts+default_diff)/gdp_usd))) %>%
        mutate(annual_impacts_scaled = annual_impacts *multiplier)%>%
        mutate(annual_impacts = annual_impacts_scaled) 
      
      D0_htf <- data_i %>%
        filter(sector %in% 'High Tide Flooding and Traffic') %>%
        mutate(default_diff = annual_impacts - annual_impacts[which(sectorprimary==1)]) %>%
        mutate(multiplier = 1/(1+((D0_primary$annual_impacts+default_diff)/gdp_usd))) %>%
        mutate(annual_impacts_scaled = annual_impacts *multiplier)%>%
        mutate(annual_impacts = annual_impacts_scaled)
      
      data_i_scaled <- rbind(D0_elec, D0_rail, D0_roads, D0_coastal, D0_htf)
      data_i_scaled <- data_i_scaled %>%
        select(c("sector","variant","year","trial","damageType","annual_impacts","multiplier"))#,"annual_impacts_scaled"))
      ### Return data
      return(data_i_scaled)
    }) %>%
    ### Bind the data together
    (function(x){
      do.call(rbind, x)
    }); df_fraw_2090adapt %>% glimpse
  ### Save file
  df_fraw_2090adapt %>%
    write_parquet(outputsPath %>% file.path("impacts_adapt_2090_constrained.parquet"))
} 


# 3B - baseline statistics for 2090 and adaptation sector across all trials (summed across impact types) 
# Table A3
df_fstat_2090adapt <- df_fraw_2090adapt %>%
  group_by_at(.vars = c('sector','year','variant','damageType')) %>%
  summarize(X025=quantile(annual_impacts,probs=0.025), 
            X50=quantile(annual_impacts, probs=0.50),
            X975=quantile(annual_impacts,probs=0.975),
            X005=quantile(annual_impacts,probs=0.005),
            X995=quantile(annual_impacts,probs=0.995),
            mean=mean(annual_impacts)) %>% ungroup

df_fstat_2090adapt %>%
  write_parquet(outputsPath %>% file.path("impacts_adapt_2090_stats_constrained.parquet"))



###### Physical Impacts ######
##4. Physical Impacts (Table 2)

#Physical Impacts for the year 2090
load_raw = 1
reload <- ifelse(file.exists(outputsPath %>% file.path("phys_impacts_nat_2090_constrained.parquet")) 
                 & load_raw ==0,1,0)

if (reload ==1){
  df_fraw_phys2090nat <- read_parquet(outputsPath %>% file.path("phys_impacts_nat_2090_constrained.parquet"))
}else{
  df_fraw_phys2090nat <- 
    #pblapply(1:1, function(i){
    pblapply(1:length(c_iteration), function(i){
      ### File name
      infile_i  <- inputsPath %>%
        file.path("damages", "damages") %>%
        paste(c_iteration[i], sep="_") %>%
        paste0(".", "parquet")
      ### Read in data and return
      data_i    <- infile_i %>% read_parquet
      ### Filter data for model type, national total, desired sectors, baseline scenario
      data_i    <- data_i   %>% 
        filter(model %in% c("Average", "Interpolation")) %>% #filters model type
        filter(region =='National Total') %>%                #filter for national region
        filter(sectorprimary==1) %>%                         #filters for primary variant
        filter(!sector %in% excluded_sectors) %>%            #removes sectors not needed
        filter(damageType =='Baseline') %>%                  #selects only baseline case
        filter(physicalmeasure %in% c('Premature Mortality','Crimes','Hours Lost')) %>% #select only physical measures we want
        filter(year==2090) %>%                               #select year 2090 only
        # sum across impact type and physical measure (e.g., sums different physical impact types)
        group_by_at(.vars = c_select_rawCols[!(c_select_rawCols %in% c("impactType"))]) %>%
        summarize_at(.vars = c("physical_impacts"), sum, na.rm = TRUE) %>%
        ungroup %>%
        select(-c('model_type','sectorprimary','variant','region','driverType','driverValue')) %>%
        ### Return data
        return(data_i)
    }) %>%
    ### Bind the data together
    (function(x){
      do.call(rbind, x)
    }); df_fraw_phys2090nat %>% glimpse
  ### Save file
  df_fraw_phys2090nat %>%
    write_parquet(outputsPath %>% file.path("phys_impacts_nat_2090_constrained.parquet"))
}

# 4B - baseline statistics for 2090 and physical impact sectors across all trials (summed across impact types) 
# 
df_fstat_2090phys <- df_fraw_phys2090nat %>%
  group_by_at(.vars = c('sector','year','damageType','physicalmeasure')) %>%
  summarize(X025=quantile(physical_impacts,probs=0.025), 
            X50=quantile(physical_impacts, probs=0.50),
            X975=quantile(physical_impacts,probs=0.975),
            X005=quantile(physical_impacts,probs=0.005),
            X995=quantile(physical_impacts,probs=0.995),
            mean=mean(physical_impacts)) %>% ungroup

df_fstat_2090phys %>%
  write_parquet(outputsPath %>% file.path("phys_impacts_nat_2090_stats_constrained.parquet"))



###### Damage Share of GDP ######
#### Figure 5 baseline damage GDP percent data

load_raw = 1
reload <- ifelse(file.exists(outputsPath %>% file.path("Damage_GDP_Share_2100_constrained.parquet")) 
                 & load_raw ==0,1,0)

if (reload ==1){
  df_fraw_baseline_pct <- read_parquet(outputsPath %>% file.path("Damage_GDP_Share_2100_constrained.parquet"))
}else{
  #data file created from 3_scghg.R script
  scghg_data <- read_parquet(file.path(scghgPath,
                            "co2_full_streams_2020_national_default_adaptation.parquet"))
  
  df_fraw_baseline_pct <-scghg_data %>%
      filter(year <= 2100) %>%
      filter(discount.rate == "1.5% Ramsey") %>% #doesn't matter what's used here, just filtering for duplicates
      select(year, trial, all_of("damages.baseline.pct"),"damages.baseline.pct.original")
  
  df_fraw_baseline_pct %>% 
    write_parquet(outputsPath %>% file.path("Damage_GDP_Share_2100_constrained.parquet"))
  
    
}

###### SCGHG Data ##### (path is to new constrained data)

load_raw = 1
reload <- ifelse(file.exists(outputsPath %>% file.path("scghg_by_trial_constrained.parquet")) 
                 & load_raw ==0,1,0)

if (reload ==1){
  df_fraw_baseline_pct <- read_parquet(outputsPath %>% file.path("scghg_by_trial_constrained.parquet"))
}else{
  #data file created from 3_scghg.R script
  scghg_data <- read_parquet(file.path(scghgPath,
                                       "co2_full_streams_2020_national_default_adaptation.parquet"))
  
  #first remove all data from trials where baseline damages exceeded U.S. GDP (no longer occurs as of 1/5/23 with constrained damages)
  threshold = 1
  bad_trials <- unique(scghg_data[scghg_data$damages.baseline.pct >= threshold,"trial"])
  bad_trials <- pull(bad_trials, trial) #create vector from tibble
  
  #then select only necessary columns and remove all duplicates
  # (this works because the scghg value is assigned the same value across the times series)
  df_fraw_scghg <- scghg_data %>% 
    select(trial,gas,emissions.year,discount.rate, scghg) %>%
    distinct() %>% 
    filter(!trial %in% bad_trials) %>%
    arrange(trial) %>%  
    ungroup()
    
  df_fraw_scghg %>% 
    write_parquet(outputsPath %>% file.path("scghg_by_trial_constrained.parquet"))
  
}
