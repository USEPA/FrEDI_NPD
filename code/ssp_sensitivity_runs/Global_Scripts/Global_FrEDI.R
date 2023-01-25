#-------------------------------------------------------------------------------
# Program Name: Global Functions for FrEDI Initialization & Running
# Authors: Corinne Hartin, Erin McDuffie
# Last Updated: 4 August 2022
#
# Provides global functions to initialize and run FrEDI
# Functions contained:
# xxxxx

#-------------------------------------------------------------------------------

Inst_FrEDI <-function(inst_flag, branch ) {
  library("devtools")
  if ( inst_flag == TRUE){
    withr::with_libpaths(
      new = .libPaths()[1],
      devtools::install_github(
        repo = "USEPA/FrEDI",
        subdir = "FrEDI",
        type = "source",
        force = TRUE,
        ref = branch
        #ref = "SV_module"
      ))
    library("FrEDI")
  } else {
    library("FrEDI")
  }
  #print( 'Successfully installed FrEDI')
}

Inst_Feather <-function(inst_flag ) {
  library("devtools")
  if ( inst_flag == TRUE){
    devtools::install_github(
      "wesm/feather/R")
  } else {
    library("feather")
  }
  #print( 'Successfully installed FrEDI')
}

Inst_plotfuns <-function(inst_flag ) {
  if ( inst_flag == TRUE){
  install.packages("xts")
  install.packages("dygraphs")
  install.packages("maps")
  install.packages("viridis")
  install.packages("rgeos")
  install.packages("maptools")
  library("xts")
  library("dygraphs")
  library("maps")
  library("viridis")
  library("rgeos")
  library("maptools")
  } else {
    library("xts")
    library("dygraphs")
    library("maps")
    library("viridis")
    library("rgeos")
    library("maptools")
  }
}


#Run_FrEDI

