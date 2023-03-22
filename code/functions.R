# helper functions -----------------------------------------------------------------------------------------------------

## package installation & loading ######################################################################################
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

## set correct working dir if not working with RStudio #################################################################
set_wd <- function() {
  if (basename(getwd()) != "CoverCHILD") {
    old_wd <- getwd()
    setwd(dirname(dir(path = getwd(),
                      pattern = "CoverCHILD.Rproj",
                      recursive = TRUE, full.names = TRUE)))
    return(old_wd)
  }
}

## DataFrame inspection helper functions ###############################################################################

# create codebook of input df
create_codebook <- function(df, lvl_threshold=10) {
  library("tidyverse")
  summarise(df,
            across(everything(),
                   list(type = ~list(class(.)),
                        n_unique = n_distinct,
                        perc_NA = ~round(sum(is.na(.))/length(.)*100, 1),
                        min = ~if(is.numeric(.) || is.POSIXt(.) || is.Date(.)) min(., na.rm = TRUE) else NA,
                        max = ~if(is.numeric(.) || is.POSIXt(.) || is.Date(.)) max(., na.rm = TRUE) else NA,
                        # range = ~if(is.numeric(.) || is.POSIXt(.) || is.Date(.)) list(range(., na.rm = TRUE)) else NA,
                        levels = \(x) {
                          if(is.factor(x)) lvls <- levels(x)
                          else lvls <- sort(unique(na.omit(x)))
                          if(length(lvls) > lvl_threshold) return(str_glue(">{lvl_threshold} unique vals."))
                          else return(list(lvls))
                        }),
                   .names = "{.col}:::{.fn}")) %>%
    pivot_longer(everything(),
                 names_to = c("variable_name", ".value"),
                 names_pattern = "(\\w+):::(\\w+)",
                 values_transform = as.character) %>%
    mutate(across(everything(), ~str_remove_all(., "c\\(|\\)")))
}

# generate missingness report of input df
descr_mis <- function(df) {
  library("psych")
  library("tidyverse")
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

# show columns & groups with multiple values present in grouped DF
find_mult_per <- function(df, ...) {
  library("tidyverse")
  df %>%
    group_by(...) %>%
    summarise(across(everything(), n_distinct)) %>%
    select(..., where(~ is.numeric(.) && max(., na.rm = TRUE) > 1)) %>%
    ungroup() %>%
    filter(if_any(where(is.numeric), ~ . > 1))
}

# find DFs which have the respective column
find_dfs_with_col <- function(var_names, df_list = data_tidy, all_any = any) {
  names(which(sapply(names(df_list), \(x) all_any(var_names %in% names(df_list[[x]])))))
}

# filter rows in all DFs which have the column of interest
filter_dfs_with_col <- function(..., var_names, df_list = data_tidy) {
  imap(df_list, \(df, df_name) {
    if(all(var_names %in% names(df_list[[df_name]]))) filter(df, ...) %T>%
      {cat(df_name, "\n"); print(glimpse(.)); cat("\n")}
  })
}

## misc ################################################################################################################

# generate a prettier tabyl
gen_tabyl <- function(df, ...){
  library(janitor)
  df %>%
    tabyl(..., show_missing_levels = FALSE) %>%
    adorn_totals(c("row", "col")) %>%
    adorn_percentages("row") %>%
    adorn_pct_formatting(digits = 1) %>%
    adorn_ns() %>%
    adorn_title("combined")
}
