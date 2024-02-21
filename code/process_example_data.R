#trials = sample(1:10000, 200, replace=FALSE)

for( i in trials){
  #i=18
  input = read_parquet(paste0('~/shared/OAR/OAP/CCD/CSIB/FrEDI_NPD/FrEDI_NPD_2023_s3/output/damages/rffsp/co2/damages/damages_',i,'.parquet'))
  
  temp <- input %>%
    filter(damageType == "Baseline") %>%
    filter(model %in% c("Average", "Interpolation")) %>% #filters model type
    filter(sectorprimary==1) %>%                         #filters for primary variant
    filter(includeaggregate ==1) %>%
    write_parquet(paste0('./damages_',i,'.parquet'))
}
