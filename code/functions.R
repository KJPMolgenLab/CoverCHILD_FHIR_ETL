#!/usr/bin/env Rscript

# CoverCHILD helper functions
# @author: simeonplatte
# @date: 2022-12-02

# setup helpers --------------------------------------------------------------------------------------------------------
# package installation only
inst_pkgs <- function(..., silent = FALSE) {
  pkgs <- list(...)
  for (pkg in pkgs) if (identical(system.file(package = pkg), "")) {
    install.packages(pkg)
  } else {
    cat(paste0("Package '", pkg, "' already installed."), "\n")
  }
}

# package loading or installation & loading
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

# set correct working dir if not working with RStudio
set_wd <- function() {
  if (basename(getwd()) != "CoverCHILD") {
    old_wd <- getwd()
    setwd(dirname(dir(path = getwd(),
                      pattern = "CoverCHILD.Rproj",
                      recursive = TRUE, full.names = TRUE)))
    return(old_wd)
  }
}

# DataFrame inspection helpers -----------------------------------------------------------------------------------------
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

# functions with modified defaults -------------------------------------------------------------------------------------
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

# pretty printing / visualisation --------------------------------------------------------------------------------------
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

# tests for input usability --------------------------------------------------------------------------------------------
is_useful_string <- function(x) ((is.character(x) || is.factor(x)) && length(x) == 1 && !identical(x, "") && !is.na(x))
is_nonempty_df <- function(df) (is.data.frame(df) && nrow(df) != 0)

# timing via tictoc ----------------------------------------------------------------------------------------------------
tictoc_msg_prefix <- "TIMING"
tictoc_msg_pad_len <- 7L

# messages to r console
msg.tic <- function(tic, msg) {
  if (!is_useful_string(msg)) msg <- "next step"
  str_c(tictoc_msg_prefix, ": ", str_pad(round(tic, 1), tictoc_msg_pad_len), "s - Starting '", msg, "'...")
}
msg.toc <- function(tic, toc, msg) {
  if (!is_useful_string(msg)) msg <- "last step"
  str_c(tictoc_msg_prefix, ": ", str_pad(round(toc, 1), tictoc_msg_pad_len), "s - Finished '", msg, "'. ",
        round(toc-tic, 1), "s elapsed.")
}

# logging function that saves to file
ctic.log <- function(show = FALSE,
                     save = paste0("Timings_", format(Sys.time(), "%y%m%d"), ".csv"),
                     append_current_only = TRUE) {
  log_df <- map(tic.log(format = FALSE), \(x) {
    if (length(x$msg) == 0) x$msg <- NA
    as_tibble_row(x)
  }) %>%
    bind_rows() %>%
    rename(start_s = tic, finish_s = toc) %>%
    mutate(dur_s = finish_s - start_s,
           dur_m = dur_s / 60,
           .after = finish_s)
  if (show) print(log_df, n = Inf)

  if (is.character(save) && !is.na(save)) {
    if (!is_useful_string(save)) {
      stop("Saving of timing log was requested, but no valid filepath was provided.")
    } else {
      dir.create(dirname(save), showWarnings = FALSE, recursive = TRUE)
      write_excel_csv2(if(append_current_only) slice_tail(log_df, n = 1) else log_df,
                       file = save,
                       append = append_current_only,
                       col_names = (!append_current_only) || (!file.exists(save)))
    }
  }
  invisible(log_df)
}

# custom tic, toc, log with project defaults
ctic <-
  function(msg = NULL, quiet = FALSE, func.tic = msg.tic, ...) tic(msg = msg, quiet = quiet, func.tic = func.tic, ...)
ctoc <-
  function(log = TRUE, quiet = FALSE, func.toc = msg.toc, ...) toc(log = log, quiet = quiet, func.toc = func.toc, ...)
ctoc_log <- function(log = TRUE,
                     quiet = FALSE,
                     func.toc = msg.toc,
                     show = FALSE,
                     save = FALSE,
                     append_current_only = TRUE,
                     ...) {
  toc(log = log, quiet = quiet, func.toc = func.toc, ...)
  ctic.log(show = show, save = save, append_current_only = append_current_only)
}

# FHIR handling --------------------------------------------------------------------------------------------------------
## helper functions ----
# find in a df columns with entries which have multiple indices and need to be fhir_melted
find_melt_col_groups <- function(df, brackets = NULL, config = cfg) {
  if (!is.null(config)) {
    if (is.null(brackets)) brackets <- config$brackets
  }
  if (is.null(brackets)) stop("No brackets provided and could neither be read from config.")
  if (!is_nonempty_df(df)) stop("DF must be a dataframe with at least 1 row.")

  pattern <- paste0("\\", brackets[[1]], ".*[02-9].*\\", brackets[[2]])
  melt_group_df <- df %>%
    summarise(across(everything(), \(x) any(str_detect(x, pattern), na.rm = TRUE))) %>%
    data.table::transpose(keep.names = "source") %>%
    filter(V1)
  if (nrow(melt_group_df) == 0) return(list())
  melt_group_df %<>%
    separate_wider_delim(source, ".", names = paste0("grouplvl", 1:max_na(str_count(.$source, "\\."))),
                         too_few = "align_start", too_many = "drop", cols_remove = FALSE) %>%
    mutate(across(starts_with("grouplvl"), as.factor))
  melt_group_df %>% select(source, grouplvl1) %>% chop(source) %>% deframe()
  #TODO: expand to be able to use lower grouping levels. Lower lvl groups need to include higher lvls.
  #      str_split(., "\\.")...
}

# convert fhir_search_cfg$parameters resource name to actual resource name
search_name_to_resource <- function(x) {
  x <- str_to_title(str_remove(x, "[\\_\\-\\d]*$"))
  if (!is_useful_string(x)) x <- NULL
  return(x)
}

#TODO: add verbosity levels to all functions

# remove reference resource prefix of columns linking to other resources
remove_ref_prefix <- function(refs, resource = NULL, ref_col = NULL, ref_prefix = NULL, config = cfg) {
  # default hierarchy:
  # 1) use provided ref_prefix
  # 2) look up ref_prefix in config
  # 3) automatically detect ref_prefix

  # get column name if it's not provided
  if (is.null(ref_col)) {
    ref_col_name <- deparse1(substitute(refs))
    ref_col <- tryCatch(cur_column(), error = function(e) ref_col_name)
  }

  if (is_useful_string(ref_prefix)) {
    prefix_src <- "manually provided"
  } else if (!is.null(config)) {
    # try to look up ref_prefix in config
    if (is_useful_string(resource)) ref_prefix <- config$ref_prefixes[[tolower(resource)]][[ref_col]]
    if (is_useful_string(ref_prefix)) prefix_src <- "read from config"
  }
  if (!is_useful_string(ref_prefix)) {
    ref_prefix <- na.omit(dirname(refs))
    ref_prefix <- ref_prefix[ref_prefix != "."]
    ref_prefix_mode <- names(which.max(table(ref_prefix)))
    ref_prefix <- unique(ref_prefix)

    if (length(ref_prefix) == 0) {
      warning(str_glue("No prefix found in column '{ref_col}', no filtering done. Provide prefix manually in ",
                       "config file or function call if there is one that was not detected."))
      return(refs)
    } else if (length(ref_prefix) > 1) {
      warning(str_glue("More than 1 prefix found in column '{ref_col}': '", paste0(ref_prefix, collapse = "', '"),
                       "'. Using the most common: '{ref_prefix_mode}'. If that is not the wanted behaviour, ",
                       "provide correct prefix manually in config file or function call."))
      ref_prefix <- ref_prefix_mode
    }
    prefix_src <- "automatically detected"
  }
  if (!any(str_detect(refs, str_glue("^{ref_prefix}/")))) {
    stop(str_glue("Prefix '{ref_prefix}/' ({prefix_src}) not found in data. Please check!"))
  }
  cat(str_glue("Removed ({prefix_src}) reference prefix '{ref_prefix}/' in column '{ref_col}'."), "\n")
  return(str_remove(refs, str_glue("^{ref_prefix}/")))
}

## modified fhircrackr functions with default values read from config ----
#TODO: add tictoc-logging to functions. Use decorators if possible?

# create list with $url named fhir_url and (if 'use_post') $body fhir_body (else NULL) elements,
#  both with default values read from config
#TODO: factor out search_cfg
fhir_url_w_cfg <- function(search_name,
                           parameters = NULL,
                           include_cfg_params = TRUE,
                           url = NULL,
                           use_post = NULL,
                           get_all_elements = NULL,
                           bundle_size = NULL,
                           config = cfg) {
  # config
  if (!is.null(config)) {
    if (is.null(url)) url <- config$serverbase
    if (is.null(use_post)) use_post <- config$use_post
    if (is.null(get_all_elements)) get_all_elements <- config$get_all_elements
    if (is.null(bundle_size)) bundle_size <- config$bundle_size
  }

  resource <- search_name_to_resource(search_name)
  parameters = as.list(c(parameters,
                         if (include_cfg_params) search_cfg$named_params[[search_name]],
                         "_elements" = if (!get_all_elements) {
                           elements <- paste0(search_cfg$base_elements[[search_name]], collapse = ",")
                           if (identical(elements, "")) NULL else elements
                         },
                         "_count" = as.character(bundle_size)))

  if (!use_post) {
    search_url <- fhir_url(url = url, resource = resource, parameters = parameters)
    search_body <- NULL
  } else {
    search_url <- fhir_url(url = url, resource = resource)
    search_body <- fhir_body(content = parameters)
    attr(search_body, "names") <- search_name
    attr(search_body, "search_name") <- search_name
    attr(search_body, "resource") <- resource
  }
  names(search_url) <- search_name
  attr(search_url, "search_name") <- search_name
  attr(search_url, "resource") <- resource
  return(list(url = search_url, body = search_body))
}

fhir_url_to_url_w_cfg <- function(search_url, search_body = NULL, verbose = 1) {
  if (is_useful_string(search_url)) {
    search_url <- list(url = search_url, body = search_body)
  } else if (is.list(search_url) && is_useful_string(search_url[["url"]])) {
    if ((class(search_body) == "fhir_body") && verbose > 0) message(
      "'search_url' is a fhir_url_w_cfg-like list, therefore the provided 'search_body' will be ignored ",
      "and the list-body will be used (even if it is NULL).")
  } else {
    if (verbose > 0) warning(str_glue("Provided 'search_url' '{format(search_url)}' not useable. Please provide ",
                                      "either a fhir_url-object (and optionally a body) or a fhir_url_w_cfg-like ",
                                      "list with 'url' and optionally 'body'. Returning NULL."))
    search_url <- NULL
  }
  return(search_url)
}

save_to_disc_path_w_cfg <- function(save_to_disc, save_type = "DFs", search_name = NULL, config = cfg, verbose = TRUE) {
  if (isTRUE(save_to_disc) || is_useful_string(save_to_disc)) {
    if (!is_useful_string(search_name)) search_name <- tempfile(pattern = "", tmpdir = "")
    if (isTRUE(save_to_disc)) {
      root_dir <- if(!is.null(config)) config$tmp_dir
      if (is.null(root_dir)) root_dir <- tempdir()
      save_to_disc <-
        file.path(root_dir, str_glue("FHIR_{save_type}_{search_name}_", format(Sys.time(), "%y%m%d_%H%M%S")))
    }
    dir.create(save_to_disc, showWarnings = FALSE, recursive = TRUE)
    if (verbose > 0) {
      cat(str_glue("Downloaded {save_type} of this '{search_name}' FHIR search will be saved to: {save_to_disc}"), "\n")
    }
    return(save_to_disc)
  } else {
    return(NULL)
  }
}

# fhir_search with defaults: values read from config, log_errors path, save_to_disc path
fhir_search_w_cfg <- function(search_url = NULL,
                              search_body = NULL,
                              username = NULL,
                              password = NULL,
                              token = NULL,
                              max_bundles = NULL,
                              batch_mode = FALSE,
                              verbose = 2L,
                              log_errors = TRUE, # TRUE/NULL, or string for path to logfile
                              save_to_disc = NULL,
                              config = cfg,
                              search_name = NULL) {
  # config
  if (!is.null(config)) {
    if (is.null(username)) username <- config$username
    if (is.null(password)) password <- config$password
    if (is.null(token)) token <- config$token
    if (is.null(max_bundles)) max_bundles <- if (batch_mode) config$bundles_per_batch else config$max_bundles
  }

  # search_url default hierarchy
  # 1) provided by function arguments
  url_list <- fhir_url_to_url_w_cfg(search_url, search_body, verbose = 0)
  if (!is_useful_string(url_list[["url"]])) {
    # 2) from search_name
    if (is_useful_string(search_name) && !is.null(fhir_search_urls[[search_name]])) {
      url_list <- fhir_url_to_url_w_cfg(fhir_search_urls[[search_name]], verbose = 0)
      if (is_useful_string(url_list[["url"]])) {
        if (verbose > 1) message(str_glue(
          "Arguments 'search_url' and 'search_body' not useable, but found fhir_url {body_msg} from ",
          "provided search_name '{search_name}', which will be used.",
          body_msg = if (is.null(url_list[["body"]])) "" else "and fhir_body"))
      }
    }
    # 3) from fhir_current_request
    if (!is_useful_string(url_list[["url"]])) {
      if (verbose > 1) message("Neither search_url nor search_name useable, falling back on URL from ",
                               "'fhir_current_request()'.")
      url_list <- fhir_url_to_url_w_cfg(fhir_current_request(), verbose = 0)
    }
  }
  search_url <- url_list[["url"]]
  search_body <- url_list[["body"]]

  # set name & resource of search
  resource <- attr(search_url, "resource")
  if (is.null(resource)) resource <- search_name_to_resource(names(search_url))
  if (is.null(resource)) resource <- search_name_to_resource(attr(search_url, "search_name"))
  if (is.null(resource)) resource <- str_extract(search_url, ".*\\/(\\w+)\\??", group = 1)
  if (is.null(search_name)) search_name <- names(search_url)
  if (is.null(search_name)) search_name <- attr(search_url, "search_name")
  if (is.null(search_name)) search_name <- resource

  # create default path to logfile for http errors
  if (isTRUE(log_errors)) log_errors <-
      file.path(config$log_dir,
                paste0("FHIR_search_errors_", search_name, "_", format(Sys.time(), "%y%m%d-%H%M"), ".log"))
  if (is_useful_string(log_errors)) {
    dir.create(dirname(log_errors), showWarnings = FALSE, recursive = TRUE)
    if (verbose > 0) cat("HTTP errors for this '", search_name, "' FHIR search will be saved to: ", log_errors,
                         "\n", sep = '')
  }

  # create default folder to save downloaded bundles to disc
  save_to_disc <- save_to_disc_path_w_cfg(save_to_disc = save_to_disc,
                                          save_type = "bundles",
                                          search_name = search_name,
                                          config = config,
                                          verbose = verbose)

  x <- fhir_search(request = search_url,
                   body = search_body,
                   username = username,
                   password = password,
                   token = token,
                   max_bundles = max_bundles,
                   verbose = verbose,
                   log_errors = log_errors,
                   save_to_disc = save_to_disc)
  if (is.null(x) && is_useful_string(save_to_disc)) {
    x <- save_to_disc
    names(x) <- "save_to_disc_path"
  }
  attr(x, "search_name") <- search_name
  attr(x, "resource") <- resource
  return(x)
}

# fhir_crack with defaults: values read from config, log_errors path, save_to_disc path
#TODO: factor out fhir_design_w_cfg, allow to crack multiple resources (right now only main resource is cracked)
#TODO: factor out search_cfg
fhir_crack_w_cfg <- function(bundles = NULL,
                             design = NULL,
                             rm_empty_cols = NULL,
                             data.table = FALSE,
                             format = NULL,
                             keep_attr = NULL,
                             verbose = 2,
                             sep = NULL,
                             brackets = NULL,
                             keep_all_elements = NULL,
                             n_cores = NULL,
                             config = cfg,
                             search_name = NULL,
                             resource = NULL) {
  # config
  if (!is.null(config)) {
    if (is.null(sep)) sep <- config$sep
    if (is.null(brackets)) brackets <- config$brackets
    if (is.null(keep_all_elements)) keep_all_elements <- config$keep_all_elements
    if (is.null(n_cores)) n_cores <- config$n_cores
  }

  # bundle default hierarchy
  # 1) provided by function arguments
  if (is.null(bundles)) {
    # 2) from search_name
    if (is_useful_string(search_name) && !is.null(fhir_bundles[[search_name]])) {
      if (verbose > 1) message(
        str_glue("Argument bundles not provided, bundles from provided search_name '{search_name}' will be used."))
      bundles <- fhir_bundles[[search_name]]
    } else {
      stop("Neither bundles argument nor search_name yielded bundles, stopping.")
    }
  } else {
    if (is_useful_string(search_name)) {
      if (is_useful_string(attr(bundles, "search_name")) && verbose > 1) message(str_glue(
        "Provided search_name '{search_name}' taking precedence over search_name '{bundle_name}' read from bundle.",
        bundle_name = attr(bundles, "search_name")))
    } else {
      search_name <- attr(bundles, "search_name")
    }
  }

  # resource default hierarchy
  # 1) from design
  if (!is.null(design)) {
    if (class(design) %in% c("fhir_table_description", "fhir_design")) {
      if (is_useful_string(resource)) message(
        "Provided both design and resource arguments, resource from design taking precedence over resource argument.")
      if (class(design) == "fhir_table_description") {
        resource <- design@resource@.Data
      } else if (class(design) == "fhir_design") {
        stop("Design for multiple tables (class fhir_design) not yet implemented.") #TODO
        resource <- map_chr(design@.Data, \(x) x@resource@.Data)
      }
    } else {
      warning("Provided design was neither a fhir_table_description nor a fhir_design, ",
              "trying to create design from config.")
    }
  }
  # 2) from resource argument
  if (is_useful_string(resource)) {
    if (is_useful_string(attr(bundles, "resource"))) message(
      str_glue("Resource '{resource}' takes precedence over resource read from bundle."))
  } else {
    # 3) from bundles
    resource <- attr(bundles, "resource")
  }

  # check that either search_name or resource is present
  if (is_useful_string(resource)) {
    if (!is_useful_string(search_name)) search_name <- resource
  } else {
    if (is_useful_string(search_name)) {
      resource <- search_name_to_resource(search_name)
    } else {
      stop("search_name/resource was not provided and could not be derived from design, ",
           "bundles or search_name arguments, stopping.")
    }
  }

  # create design if not provided
  if (is.null(design)) {
    if (verbose > 1) message(str_glue("Creating design from config for resource '{resource}'."))
    design <- fhir_table_description(resource = resource,
                                     cols = if (!keep_all_elements) tryCatch(
                                       search_cfg$elements[[search_name]],
                                       error = \(e) search_cfg$elements[[tolower(resource)]]))
  }

  x <- fhir_crack(bundles = bundles, design = design, sep = sep, brackets = brackets, rm_empty_cols = rm_empty_cols,
                  verbose = verbose, data.table = data.table, format = format, keep_attr = keep_attr, ncores = n_cores)
  attr(x, "search_name") <- search_name
  attr(x, "resource") <- resource
  return(x)
}

# fhir_melt with defaults from config
fhir_melt_w_cfg <- function(indexed_df = NULL,
                            cols = NULL,
                            sep = NULL,
                            brackets = NULL,
                            id_name = NULL,
                            all_columns = TRUE,
                            config = cfg,
                            search_name = NULL) {
  # config
  if (!is.null(config)) {
    if (is.null(sep)) sep <- config$sep
    if (is.null(brackets)) brackets <- config$brackets
  }

  # indexed_df default hierarchy
  # 1) from argument
  if (!is_nonempty_df(indexed_df)) { # if indexed_df not useable
    # 2) from search_name
    if (is_useful_string(search_name) && is_nonempty_df(fhir_dfs[[search_name]])) {
      message(str_glue("Argument indexed_df not usable or provided, ",
                       "DF from provided search_name '{search_name}' will be used."))
      indexed_df <- fhir_dfs[[search_name]]
    } else {
      warning("'indexed_df' not usable and could not be read from search_name. No melting done, ",
              "object returned unmodified.")
      return(indexed_df)
    }
  }

  # search_name default hierarchy
  # 1) from argument
  # 2) from indexed_df attribute
  if (!is_useful_string(search_name)) search_name <- attr(indexed_df, "search_name")
  # 3) from indexed_df name
  if (is.null(search_name)) search_name <- deparse1(substitute(indexed_df))

  # resource default hierarchy
  resource <- attr(indexed_df, "resource")
  if (!is_useful_string(resource)) resource <- search_name_to_resource(search_name)

  if (is.null(cols)) {
    cols <- find_melt_col_groups(indexed_df, brackets)
    if (length(cols) == 0) {
      warning(str_glue("Separator '{sep}' not found in DF '{search_name}', returning unmodified DF."))
      return(indexed_df)
    }
    if (length(cols) > 1) message(str_glue("Found {length(cols)} melt groups for FHIR DF '{search_name}': '",
                                           paste0(names(cols), collapse = "', '"),
                                           "'. Using only the first group."))
    cols <- cols[1]
  }

  melt_group_name <- names(cols)[1]
  if (!is_useful_string(id_name) && is_useful_string(melt_group_name)) {
    id_name <- paste0("resource_id_", melt_group_name)
  } else {
    id_name <- "resource_id"
  }
  if (is.list(cols)) cols <- cols [[1]]

  cat(str_glue("Melting DF '{search_name}', group{melt_group_name_msg}: '", paste0(cols, collapse = "', '"), "'.",
               melt_group_name_msg = if (!is.null(melt_group_name)) {
                 str_glue(" from attribute '{melt_group_name}'")
               } else ""), "\n")
  indexed_df <- fhir_melt(indexed_data_frame = indexed_df, columns = cols, brackets = brackets, sep = sep,
                          id_name = id_name, all_columns = all_columns)
  attr(indexed_df, "search_name") <- search_name
  attr(indexed_df, "resource") <- resource
  return(indexed_df)
}

# fhir_melt_w_cfg loop over all existing melt_groups
fhir_melt_loop_w_cfg <- function(indexed_df = NULL,
                                 col_group_list = NULL,
                                 sep = NULL,
                                 brackets = NULL,
                                 id_name_template = NULL,
                                 all_columns = TRUE,
                                 rm_indices = TRUE,
                                 config = cfg,
                                 search_name = NULL) {
  # config
  if (!is.null(config)) {
    if (is.null(sep)) sep <- config$sep
    if (is.null(brackets)) brackets <- config$brackets
  }

  # indexed_df default hierarchy
  # 1) from argument
  if (!is_nonempty_df(indexed_df)) { # if indexed_df not useable
    # 2) from search_name
    if (is_useful_string(search_name) && is_nonempty_df(fhir_dfs[[search_name]])) {
      message(str_glue("Argument indexed_df not usable or provided, ",
                       "DF from provided search_name '{search_name}' will be used."))
      indexed_df <- fhir_dfs[[search_name]]
    } else {
      warning("'indexed_df' not usable and could not be read from search_name. No melting done, ",
              "object returned unmodified.")
      return(indexed_df)
    }
  }

  # search_name default hierarchy
  # 1) from argument
  # 2) from indexed_df attribute
  if (!is_useful_string(search_name)) search_name <- attr(indexed_df, "search_name")
  # 3) from indexed_df name
  if (is.null(search_name)) search_name <- deparse1(substitute(indexed_df))

  if (is.null(col_group_list)) col_group_list <- find_melt_col_groups(indexed_df, brackets)
  if (length(col_group_list) == 0) {
    warning(str_glue("Separator '{sep}' not found in DF{search_name_msg}, returning {rm_indices_msg}.",
                     search_name_msg = if (is_useful_string(search_name)) str_glue(" '{search_name}'") else "",
                     rm_indices_msg = if (rm_indices) "DF with only indices removed" else "unmodified DF"))
  } else {
    cat(str_glue("Found {length(col_group_list)} melt group(s) for FHIR DF '{search_name}': '",
                     paste0(names(col_group_list), collapse = "', '"),
                     "'. Melting sequentially."), "\n")
    for (melt_group in names(col_group_list)) {
      indexed_df <- fhir_melt_w_cfg(indexed_df = indexed_df,
                                    cols = col_group_list[melt_group],
                                    sep = sep,
                                    brackets = brackets,
                                    id_name = paste0("resource_id_", melt_group),
                                    all_columns = all_columns,
                                    config = config,
                                    search_name = NULL)
    }
  }
  if (rm_indices) indexed_df <- fhir_rm_indices(indexed_df, brackets = brackets)
  return(indexed_df)
}

# run batched fhir_search_w_cfg & fhir_crack_w_cfg, then fhir_melt_loop_w_cfg & remove_ref_prefix
#TODO: fine-res Sys.time calls for filenames, move save_path default to fhir_search_w_cfg
fhir_batched_w_cfg <- function(search_url = NULL,
                               search_body = NULL,
                               bundles_per_batch = NULL,
                               max_bundles = NULL,
                               do_melt = TRUE,
                               remove_ref_prefixes = TRUE,
                               tlog_path = NULL,
                               verbose = 2,
                               save_batches_to_disc = NULL,
                               search_name = NULL,
                               config = cfg) {
  # config
  if (!is.null(config)) {
    if (is.null(bundles_per_batch)) bundles_per_batch <- config$bundles_per_batch
    if (is.null(max_bundles)) max_bundles <- config$max_bundles
    if (is.null(save_batches_to_disc)) save_batches_to_disc <- config$save_batches_to_disc
  }
  #TODO implement verbose instead of do_log, split logging & console printing
  do_log <- verbose > 0

  # search_url default hierarchy
  # 1) provided by function arguments
  url_list <- fhir_url_to_url_w_cfg(search_url, search_body, verbose = 0)
  if (!is_useful_string(url_list[["url"]])) {
    # 2) from search_name
    if (is_useful_string(search_name) && !is.null(fhir_search_urls[[search_name]])) {
      url_list <- fhir_url_to_url_w_cfg(fhir_search_urls[[search_name]], verbose = 0)
      if (is_useful_string(url_list[["url"]])) {
        if (verbose > 1) message(str_glue(
          "Arguments 'search_url' and 'search_body' not useable, but found fhir_url {body_msg} from ",
          "provided search_name '{search_name}', which will be used.",
          body_msg = if (is.null(url_list[["body"]])) "" else "and fhir_body"))
      }
    }
    if (!is_useful_string(url_list[["url"]])) {
      warning("Neither search_url nor search_name useable, falling back on URL from 'fhir_current_request()'. ",
              "This only works for GET-requests as HTML bodies are not included.")
      url_list <- fhir_url_to_url_w_cfg(fhir_current_request(), verbose = 0)
    }
  }

  # resource default hierarchy
  resource <- attr(url_list[["url"]], "resource")
  if (is.null(resource)) resource <- search_name_to_resource(names(url_list[["url"]]))
  if (is.null(resource)) resource <- str_extract(url_list[["url"]], ".*\\/(\\w+)\\??", group = 1)
  # search_name default hierarchy
  if (!is_useful_string(search_name)) search_name <- names(url_list[["url"]])
  if (!is_useful_string(search_name)) search_name <- attr(url_list[["url"]], "search_name")
  if (!is_useful_string(search_name)) search_name <- resource

  if (do_log) ctic(str_glue("Batched FHIR search: {search_name} - Download & crack bundles"))

  # set path for saving downloaded files
  save_path <- save_to_disc_path_w_cfg(save_to_disc = save_batches_to_disc,
                                       save_type = "cracked_DFs",
                                       search_name = search_name,
                                       config = config,
                                       verbose = verbose)
  save_batches_to_disc <- is_useful_string(save_path)
  save_mask <- file.path(save_path, paste0("DF_", search_name, "_{id}.rds"))

  # enforce max_bundles as upper limit, even if bundles_per_batch would be larger
  if (max_bundles < bundles_per_batch) bundles_per_batch <- max_bundles
  fhir_page_count <- 1
  downloaded_bundle_count <- 0
  fhir_dfs <- list()
  if (verbose > 0) cat(str_glue("Batched settings: Processing {bundles_per_batch} bundles per batch, ",
                                "maximum of {max_bundles} bundles in total."), "\n")
  while(is_useful_string(url_list[["url"]]) &&
        (bundles_per_batch > 0) &&
        ((downloaded_bundle_count + bundles_per_batch) <= max_bundles)) {
    # search
    if (do_log) ctic(str_glue("Batched FHIR search: {search_name} - Download bundle set {fhir_page_count}"))
    fhir_bundles <- fhir_search_w_cfg(search_url = url_list,
                                      max_bundles = bundles_per_batch,
                                      verbose = verbose,
                                      search_name = search_name,
                                      config = config)
    if (do_log) ctoc_log(save = tlog_path)

    # crack
    if (do_log) ctic(str_glue("Batched FHIR search: {search_name} - Crack bundle set {fhir_page_count}"))
    fhir_dfs[[fhir_page_count]] <- fhir_crack_w_cfg(bundles = fhir_bundles, verbose = verbose, config = config)
    rm(fhir_bundles)
    gc(); gc()
    if (do_log) ctoc_log(save = tlog_path)

    # melt if requested
    if (do_melt) {
      if (do_log) ctic(str_glue("Batched FHIR search: {search_name} - Melt DF"))
      fhir_dfs[[fhir_page_count]] <- fhir_melt_loop_w_cfg(indexed_df = fhir_dfs[[fhir_page_count]], config = config)
      if (do_log) ctoc_log()
    }

    # save if requested
    if (save_batches_to_disc) {
      saveRDS(fhir_dfs[[fhir_page_count]], str_glue(save_mask, id = fhir_page_count))
      fhir_dfs[fhir_page_count] <- NULL
      gc(); gc()
    }

    url_list[["url"]] <- fhir_next_bundle_url()
    if (is_useful_string(url_list[["url"]])) names(url_list[["url"]]) <- search_name
    url_list[["body"]] <- NULL

    downloaded_bundle_count <- downloaded_bundle_count + bundles_per_batch
    cat(str_glue("Processed {downloaded_bundle_count} bundles so far. Stopping after {max_bundles} bundles max."),
        "\n")
    # enforce max_bundles as upper limit if it would be exceeded in the next batch
    if (max_bundles < (downloaded_bundle_count + bundles_per_batch)) {
      bundles_per_batch <- max_bundles - downloaded_bundle_count
    }
    fhir_page_count <- fhir_page_count + 1
  }
  if (do_log) ctoc_log(save = tlog_path)

  # load downloaded FHIR DFs if requested
  if (do_log) ctic(str_glue("Batched FHIR search: {search_name} - Combine downloaded DFs"))
  if (save_batches_to_disc) fhir_dfs <- map(Sys.glob(str_glue(save_mask, id = "*")), readRDS)
  fhir_dfs <- bind_rows(fhir_dfs)
  if (do_log) ctoc_log()

  # remove reference prefixes if requested
  if (remove_ref_prefixes) {
    if (do_melt) {
      if (is_nonempty_df(fhir_dfs)) fhir_dfs <- fhir_dfs %>%
          {mutate(., across(contains("reference"),
                            \(x) remove_ref_prefix(refs = x, resource = attr(., "resource"), config = config)))}
    } else {
      warning("Reference prefix removal was requested, but they will only be removed from a molten DF. Melting was ",
              "not requested so prefix removal will not be done (remove_ref_prefixes=T, but do_melt=F).")
    }
  }

  gc(); gc()
  return(fhir_dfs)
}
