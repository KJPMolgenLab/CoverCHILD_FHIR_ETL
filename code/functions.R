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
  if(!("tidyverse" %in% loadedNamespaces())) library(tidyverse)
  summarise(df,
            across(everything(),
                   list(type = ~list(class(.)),
                        n_unique = n_distinct,
                        perc_NA = ~round(sum(is.na(.))/length(.)*100, 1),
                        min = ~if(is.numeric(.) || is.POSIXt(.) || is.Date(.)) min_na(.) else NA,
                        max = ~if(is.numeric(.) || is.POSIXt(.) || is.Date(.)) max_na(.) else NA,
                        mode = \(x) na.omit(x) %>% {
                          if (length(.) == 0) NA
                          else as.factor(.) %>%
                            fct_count(sort = TRUE) %>%
                            use_series(f) %>%
                            extract2(1) %>%
                            as.character()
                          },
                        # range = ~if(is.numeric(.) || is.POSIXt(.) || is.Date(.)) list(range(., na.rm = TRUE)) else NA,
                        levels = \(x) {
                          if(is.factor(x)) lvls <- levels(x)
                          else lvls <- sort(unique(na.omit(x)))
                          if(length(lvls) > lvl_threshold) return(str_glue(">{lvl_threshold} unique vals."))
                          else return(list(lvls))
                        }),
                   .names = "{.col}:::{.fn}")
            ) %>%
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

# grep for column names of a DF
colname_grep <- function(df, ..., value = TRUE) {
  grep(..., x = names(df), value = value)
}

## misc ################################################################################################################
# function versions with na.rm=T as default
sum_na <- function(..., na.rm = TRUE) sum(..., na.rm = na.rm)
mean_na <- function(..., na.rm = TRUE) mean(..., na.rm = na.rm)
n_distinct_na <- function(..., na.rm = TRUE) n_distinct(..., na.rm = na.rm)
sd_na <- function(..., na.rm = TRUE) DescTools::SD(..., na.rm = na.rm)
# min & max, which return NA instead of (-)Inf, if all values are NA
min_na <- function(..., na.rm = TRUE) min(..., na.rm = na.rm) %>% {if_else(is.infinite(.), NA, .)}
max_na <- function(..., na.rm = TRUE) max(..., na.rm = na.rm) %>% {if_else(is.infinite(.), NA, .)}

# collapse multiple values to list or string, returning NA if empty
collapse_na <- function(x, sum_fun = "glue", ...) {
  if(sum_fun == "glue") sum_fun <-
      function(x, sep = ", ", width = Inf, last = "") glue_collapse(x, sep = sep, width = width, last = last)
  else if(sum_fun == "list") sum_fun <- list
  na.omit(x) %>% {if(length(.) == 0) NA else sum_fun(unique(.), ...)}
}

# generate a prettier tabyl
gen_tabyl <- function(df, ...){
  library(janitor)
  df %>%
    tabyl(..., show_missing_levels = FALSE) %>%
    adorn_totals(c("row", "col")) %>%
    adorn_percentages("col") %>%
    adorn_pct_formatting(digits = 2) %>%
    adorn_ns(format_func = \(x) format(x)) %>%
    adorn_title("combined")
}

# rgb() function with maxColorValue = 255 as default
rgb256 <- function(..., maxColorValue = 255) rgb(..., maxColorValue = maxColorValue)

# factor labeller: use only every nth label for plotting
label_seq <- function(fac, n) {
  lvls <- levels(droplevels(fac))
  return(lvls[seq(1, length(lvls), n)])
}
