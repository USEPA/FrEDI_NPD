#####
## Title:  3_compute_npd_damage_transformation.R
## Purpose: This file reads in FrEDI results from all RFF scenarios [trials] and
##          calculates the NPD value ($ per ton CO2) using 
##          1.5%, 2%, 2.5%, and 3% Ramsey discounting and a 2% and 3% constant discount rate
##          by 2300 in 2020 USD (marginal = perturbed - baseline).
##          - FrEDI annual damages are converted from 2015$ to 2020$
## Inputs:  data/external/rffsp_usa.csv
##          data/external/rffsp_fair_sequence.csv
##          output/damages/rffsp/damages/damages_[scenario].parquet
## Outputs: output/npd/co2_full_streams_2020_national_default_adaptation.parquet
##          output/npd/npd_fredi_national.csv
## Written by: US EPA, National Center for Environmental Economics (OP) and Climate Change Division (OAP); December 2022
## Last updated: 1.4.2023 by B. Parthum
## NOTES: 
#####

##########################
#################  library
##########################

## clear workspace
rm(list = ls())
gc()

## this function will check if a package is installed, and if not, install it
list.of.packages = c('tidyverse', 'magrittr',
                     'arrow',
                     'zoo',
                     'doParallel', 'foreach',
                     'ggplot2')
new.packages = list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages, repos = "http://cran.rstudio.com/")
lapply(list.of.packages, library, character.only = TRUE)

##########################
###################  parts
##########################

## adaptation assumptions
ADAPT = 'default'

## relevant fredi columns
fredi.columns = c('sector', 'variant', 'region', 'year', 'model_type', 'sectorprimary', 'physicalmeasure', 'trial', 'damageType')

## function to process fredi output (for default adaptation assumptions)
## also aggregate across ImpactTypes, convert to 2020$, and calculate marginal impacts
read_fredi = 
  function(x){
    read_parquet(x) %>%
      filter(variant %in% c('Reactive Adaptation', 'N/A', '2011 Emissions', 'With CO2 Fertilization','Mean','Reasonably Anticipated Adaptation'),
             sector  %in% c('Air Quality', 'CIL Agriculture', 'CIL Crime', 'ATS Extreme Temperature', 'Coastal Properties', 'Electricity Demand and Supply',
                            'Electricity Transmission and Distribution', 'High Tide Flooding and Traffic', 'Inland Flooding', 'Labor', 'Marine Fisheries', 'Rail',
                            'Roads', 'Southwest Dust', 'Urban Drainage', 'Valley Fever', 'Water Quality', 'Wildfire', 'Wind Damage', 'Winter Recreation'),
             region  %in% c('National Total'),
             model   %in% c('Average', 'Interpolation'),
             year >= 2020) %>% 
      group_by_at(fredi.columns) %>%
      summarize(annual_impacts = sum(annual_impacts, na.rm = T), 
                .groups = 'drop') %>%
      pivot_wider(names_from  = 'damageType', 
                  values_from = "annual_impacts") %>%
      rename(damages.baseline  = Baseline,
             damages.perturbed = Perturbed) %>%
      select(trial, year, damages.baseline, damages.perturbed) %>% 
      group_by(trial, year) %>%     
      summarise(damages.baseline  = sum(damages.baseline) * pricelevel_2015_to_2020,
                damages.perturbed = sum(damages.perturbed) * pricelevel_2015_to_2020,
                .groups = 'drop') %>% 
      mutate(damages.marginal = (damages.perturbed - damages.baseline) * (12/44) * (1e-9),  ## convert C to CO2 (12/44) and 1Gt to 1mt (1e-9) (i.e., convert from FaIR units)
             emissions.year   = YEAR,
             gas              = GAS)
  }

##########################
####################  data
##########################

## implicit price deflators from BEA Table 1.1.9; recovered 1/13/2022: https://apps.bea.gov/iTable/iTable.cfm?reqid=19&step=3&isuri=1&select_all_years=0&nipa_table_list=13&series=a&first_year=2011&last_year=2015&scale=-99&categories=survey&thetable=
pricelevel_2011_to_2020 = 113.648/98.164  ## rffsps are in 2011$
pricelevel_2015_to_2020 = 113.648/104.691 ## fredi is in 2015$

## read rffsps for us
## rffsp_fair_sequence includes the cross-walk between trial, rffsp ID, and fair ID
rffsp = 
  read_csv('data/external/rffsp_usa.csv') %>% 
  select(rffsp.id, year, pop, gdp) %>% 
  group_by(rffsp.id) %>%
  complete(year = seq(first(year), last(year))) %>% 
  mutate(pop = exp(na.approx(log(pop))) * 1e3,
         gdp = exp(na.approx(log(gdp))) * pricelevel_2011_to_2020 * 1e6) %>% 
  right_join(read_csv('data/external/rffsp_fair_sequence.csv') %>% 
               select(trial, rffsp.id),
             by = 'rffsp.id') %>% 
  arrange(trial, year)

##########################
######### baseline damages
##########################

## start the clock
time1 = Sys.time()

## object to store data
means = tibble()

for (GAS in c('co2')) {
  for (YEAR in c(2020)) {
    
    # ## test
    # GAS  = 'co2'
    # YEAR = 2020
    
    ## damages files, output from 2_run_fredi.r
    damages = 
      list.files('output/damages/rffsp/damages', full.names = T) %>% 
      map_df(~read_fredi(.))
    
    ## recover rffsp gdp and pop 
    damages %<>% 
      left_join(rffsp, by = c('trial', 'year'))
    
    ## get baseline damages as percent of exogenous gdp and income per capita (ypc)
    damages %<>% 
      group_by(trial, gas, emissions.year) %>% 
      mutate(damages.baseline.pct.original = damages.baseline/gdp,
             damages.baseline.pct  = 1 - (1/(1+(damages.baseline/gdp))),
             damages.perturbed.pct = 1 - (1/(1+(damages.perturbed/gdp))),
             ypc                   = ((1-damages.baseline.pct) * gdp)/pop,
             base.ypc              = case_when(year == emissions.year ~ ypc, T ~ 0),
             base.ypc              = max(base.ypc), ## TO-DO, clunky way to ensure that the base ypc for discrete time discount factor can be used in the discounting function, fix later
             damages.baseline      = gdp * damages.baseline.pct,
             damages.perturbed     = gdp * damages.perturbed.pct,
             damages.marginal      = (damages.perturbed - damages.baseline) * (12/44) * (1e-9)) %>% 
      ungroup()
    
    ## test damage transformation
    dif =
      damages %>%
      group_by(year) %>%
      summarize(damage.fraction.difference         = mean(damages.baseline.pct.original - damages.baseline.pct) * -1,
                damage.fraction.difference.med     = median(damages.baseline.pct.original - damages.baseline.pct) * -1,
                damage.fraction.difference.q05     = quantile(damages.baseline.pct.original - damages.baseline.pct, 0.05) * -1,
                damage.fraction.difference.q95     = quantile(damages.baseline.pct.original - damages.baseline.pct, 0.95) * -1,
                damage.fraction.difference.pct     = mean((1 - (damages.baseline.pct/damages.baseline.pct.original)), na.rm = T) * -1,
                damage.fraction.difference.pct.med = median((1 - (damages.baseline.pct/damages.baseline.pct.original)), na.rm = T) * -1,
                damage.fraction.difference.pct.q05 = quantile((1 - (damages.baseline.pct/damages.baseline.pct.original)), 0.05, na.rm = T) * -1,
                damage.fraction.difference.pct.q95 = quantile((1 - (damages.baseline.pct/damages.baseline.pct.original)), 0.95, na.rm = T) * -1)
    
    dif %>%
      ggplot() +
      geom_line(aes(year, damage.fraction.difference)) +
      geom_line(aes(year, damage.fraction.difference.med), linetype = 'dashed') +
      geom_ribbon(aes(x = year, ymin = damage.fraction.difference.q05, ymax = damage.fraction.difference.q95),
                  alpha = 0.1) +
      labs(title   = 'Absolute difference between direct and proportional damage percentages',
           y       = 'Proportional minus direct damage percentages',
           x       = 'Model Year',
           caption = 'Note: Sample from all 10,000 trials. Mean (solid) and median (dashed) lines shown along with 5th-95th percentile bounds.') +
      theme_bw()
    
    ggsave('output/npd_damage_transformation/difference_between_direct_and_proportional_damages.svg', width = 9, height = 6) 
    
    dif %>%
      ggplot() +
      geom_line(aes(year, damage.fraction.difference.pct)) +
      geom_line(aes(year, damage.fraction.difference.pct.med), linetype = 'dashed') +
      geom_ribbon(aes(x = year, ymin = damage.fraction.difference.pct.q05, ymax = damage.fraction.difference.pct.q95),
                  alpha = 0.1) +
      scale_y_continuous(labels = scales::percent_format()) +
      labs(title   = 'Percent difference between direct and proportional damage percentages',
           y       = '1 - (proportional/direct)',
           x       = 'Model Year',
           caption = 'Note: Sample from all 10,000 trials. Mean (solid) and median (dashed) lines shown along with 5th-95th percentile bounds.') +
      theme_bw()
    
    ggsave('output/npd_damage_transformation/difference_between_direct_and_proportional_damages_pct.svg', width = 9, height = 6) 
    
    ## discount rates
    rates = tibble(rate = c('1.5% Ramsey', '2.0% Ramsey', '2.5% Ramsey', '3.0% Ramsey', '2.0% CDR', '3.0% CDR'),
                   rho  = c(exp(0.000091496)-1, exp(0.001972641)-1, exp(0.004618785)-1, exp(0.007702711)-1, 0.02, 0.03), ## under discrete time, need to transform the rho that was calibrated using continuous time 
                   eta  = c(1.016010261, 1.244459020, 1.421158057, 1.567899395, 0, 0))
    
    ## object to store data
    data = tibble()
    
    ## discount marginal damages and recover 
    for (RATE in 1:length(rates$rate)){
      
      # ## test 
      # RATE = 5
      
      ## get damage parameters
      rate = rates$rate[[RATE]]
      rho  = rates$rho[[RATE]]
      eta  = rates$eta[[RATE]]
      
      ## get streams of discounted damages and net present damages
      data = 
        bind_rows(
          data,
          damages %>%
            group_by(trial, gas, emissions.year) %>%
            mutate(discount.rate               = rate,
                   discount.factor             = case_when(grepl("Ramsey", rate) ~ (base.ypc/ypc)^eta/(1+rho)^(year-emissions.year),
                                                           T ~ 1/(1+rho)^(year-emissions.year)),
                   damages.marginal.discounted = damages.marginal * discount.factor,
                   npd                       = sum(damages.marginal.discounted, na.rm = F),
                   damages.baseline.discounted = damages.baseline * discount.factor,
                   npd                         = sum(damages.baseline.discounted, na.rm = F)) %>% 
            ungroup()
        )
    }
    
    ## export full streams
    data %>% 
      write_parquet(paste0('output/npd_damage_transformation/', GAS, '_full_streams_', YEAR, '_national_', ADAPT, '_adaptation.parquet'))
    
    ## recover summary statistics
    means = 
      bind_rows(
        means, 
        data %>%
          group_by(gas, emissions.year, discount.rate) %>%
          summarise(mean        = mean(npd, na.rm = T),
                    median      = median(npd, na.rm = T),
                    `std. err.` = sd(npd, na.rm = T),
                    min         = min(npd, na.rm = T),
                    `0.5%`      = quantile(npd, .005, na.rm = T),
                    `1%`        = quantile(npd, .01,  na.rm = T),
                    `2.5%`      = quantile(npd, .025, na.rm = T),
                    `5%`        = quantile(npd, .05,  na.rm = T),
                    `10%`       = quantile(npd, .10,  na.rm = T),
                    `25%`       = quantile(npd, .25,  na.rm = T),
                    `75%`       = quantile(npd, .75,  na.rm = T),
                    `90%`       = quantile(npd, .90,  na.rm = T),
                    `95%`       = quantile(npd, .95,  na.rm = T),
                    `97.5%`     = quantile(npd, .975, na.rm = T),
                    `99%`       = quantile(npd, .99,  na.rm = T),
                    `99.5%`     = quantile(npd, .995, na.rm = T),
                    max         = max(npd),
                    .groups = 'drop') %>%
          mutate(adaptation = ADAPT)
      )
}
}
## export summary stats
means %>% 
  write_csv(paste0('output/npd_damage_transformation/npd.csv'))

### stop the clock
time2 = Sys.time()
time2 - time1

## end of script. have a great day!