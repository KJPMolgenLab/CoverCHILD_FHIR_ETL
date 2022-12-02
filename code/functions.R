# helper functions -------------------------------------------------------------

## package installation & loading ##############################################
load_inst_pkgs <- function(..., silent = FALSE){
  pkgs <- list(...) # arguments must be of type char
  for (pkg in pkgs){
    if (!require(pkg, character.only = TRUE)){
      install.packages(pkg,
                       dependencies = TRUE,
                       repos = "https://ftp.fau.de/cran/"
      )
      library(pkg, character.only = TRUE)
    }
  }
  if (!silent) cat("Done: Load libraries\n")
}

## set correct working dir if not working with RStudio #########################
set_wd <- function() {
  if (basename(getwd()) != "CoverCHILD") {
    old_wd <- getwd()
    setwd(dirname(dir(path = getwd(),
                      pattern = "CoverCHILD.Rproj",
                      recursive = TRUE, full.names = TRUE)))
    return(old_wd)
  }
}


## generate missingness report of input df #####################################
descr_mis <- function(df) {
  descr_mis_df <- df %>%
    describe(skew = FALSE) %>%
    select(-vars) %>%
    mutate(n_missing = nrow(df)-n,
           perc_missing = round(n_missing/nrow(df)*100, 1)) %>%
    relocate(n_missing, perc_missing, .after = n)
  rownames(descr_mis_df) %<>% str_remove(fixed("*"))
  descr_mis_df %<>% rownames_to_column(var = "variable")
  descr_mis_df <-
    df %>%
    summarise(across(.fns = ~list(sort(unique(na.omit(.)))))) %>%
    pivot_longer(cols = everything(),
                 names_to = "variable",
                 values_to = "unique_vals") %>%
    rowwise() %>%
    mutate(n_unique = length(unique_vals)) %>%
    ungroup() %>%
    left_join(descr_mis_df, by = "variable") %>%
    arrange(n)
  return(descr_mis_df)
}
