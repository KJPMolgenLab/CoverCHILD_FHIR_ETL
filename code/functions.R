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
    if ((length(save) == 0) || identical(save, "")) {
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
find_melt_col_groups <- function(df, brackets = cfg$brackets) {
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
  } else {
    # try to look up ref_prefix in config
    if (is_useful_string(resource)) ref_prefix <- config$ref_prefixes[[tolower(resource)]][[ref_col]]
    if (is_useful_string(ref_prefix)) {
      prefix_src <- "read from config"
    } else {
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
  }
  if (!any(str_detect(refs, str_glue("^{ref_prefix}/")))) {
    stop(str_glue("Prefix '{ref_prefix}/' ({prefix_src}) not found in data. Please check!"))
  }
  cat(str_glue("Removed ({prefix_src}) reference prefix '{ref_prefix}/' in column '{ref_col}'."), "\n")
  str_remove(refs, str_glue("^{ref_prefix}/"))
}

## modified fhircrackr functions with default values read from config ----
#TODO: add tictoc-logging to functions. Use decorators if possible?

# create list with $url named fhir_url and (if 'use_post') $body fhir_body (else NULL) elements,
#  both with default values read from config
fhir_url_w_cfg <- function(search_name,
                           parameters = NULL,
                           include_cfg_params = TRUE,
                           url = NULL,
                           use_post = NULL,
                           get_all_elements = NULL,
                           bundle_size = NULL,
                           config = cfg) {
  # config
  if (is.null(url)) url <- config$serverbase
  if (is.null(use_post)) use_post <- config$use_post
  if (is.null(get_all_elements)) get_all_elements <- config$get_all_elements
  if (is.null(bundle_size)) bundle_size <- config$bundle_size

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
    attr(search_body, "resource") <- resource
  }
  names(search_url) <- search_name
  attr(search_url, "resource") <- resource
  return(list(url = search_url, body = search_body))
}

# fhir_search with defaults: values read from config, log_errors path, save_to_disc path
fhir_search_w_cfg <- function(fhir_search_url = fhir_current_request(),
                              body = NULL,
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
  if (is.null(username)) username <- config$username
  if (is.null(password)) password <- config$password
  if (is.null(token)) token <- config$token
  if (is.null(max_bundles)) max_bundles <- if (batch_mode) config$bundles_per_batch else config$max_bundles

  if (!is.null(search_name) && !is.null(fhir_search_urls[[search_name]][["url"]])) {
    fhir_search_url <- fhir_search_urls[[search_name]][["url"]]
    body <- fhir_search_urls[[search_name]][["body"]]
    cat(str_glue("Provided search_name. Found fhir_url {body_msg1}'{search_name}' to be used, ",
                 "argument 'fhir_search_url' {body_msg2}will be ignored even if provided.",
                 body_msg1 = if (is.null(body)) "" else "and fhir_body ",
                 body_msg2 = if (is.null(body)) "" else "and 'body' "),
        "\n")
  }

  # set name & resource of search
  resource <- attr(fhir_search_url, "resource")
  if (is.null(resource)) resource <- search_name_to_resource(names(fhir_search_url))
  if (is.null(resource)) resource <- str_extract(fhir_search_url, ".*\\/(\\w+)\\??", group = 1)
  if (is.null(search_name)) search_name <- names(fhir_search_url)
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
  if (isTRUE(save_to_disc)) {
    save_to_disc <- file.path(config$tmp_dir, paste0("FHIR_bundles_", search_name, "_", format(Sys.time(), "%y%m%d")))
    if (verbose > 0) cat("Downloaded bundles of this '", search_name, "' FHIR search will be saved to: ", save_to_disc,
                         "\n", sep = '')
  }

  x <- fhir_search(request = fhir_search_url,
                   body = body,
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
  attr(x, "search_resource") <- resource
  return(x)
}

# fhir_crack with defaults: values read from config, log_errors path, save_to_disc path
fhir_crack_w_cfg <- function(bundles = NULL,
                             design = NULL,
                             sep = NULL,
                             brackets = NULL,
                             keep_all_elements = NULL,
                             rm_empty_cols = NULL,
                             verbose = 2,
                             data.table = FALSE,
                             format = NULL,
                             keep_attr = NULL,
                             n_cores = NULL,
                             config = cfg,
                             search_name = NULL,
                             resource = NULL) {
  # config
  if (is.null(sep)) sep <- config$sep
  if (is.null(brackets)) brackets <- config$brackets
  if (is.null(n_cores)) n_cores <- config$n_cores
  if (is.null(keep_all_elements)) keep_all_elements <- config$keep_all_elements

  if (!is.null(search_name) && !is.null(fhir_bundles[[search_name]])) {
    cat(str_glue("Provided 'search_name'. FHIR bundles '{search_name}' used, argument 'bundles' is ignored."), "\n")
    bundles <- fhir_bundles[[search_name]]
  }
  if (is.null(design)) {
    if (is.null(resource)) resource <- attr(bundles, "search_resource")
    if (is.null(resource)) resource <- search_name_to_resource(search_name)
    if (!is.null(resource)) {
      message(str_glue("FHIR 'design' not provided, defaulting to design for resource '{resource}' from config."))
      design <- fhir_table_description(resource = resource,
                                       cols = if (!keep_all_elements) tryCatch(
                                         search_cfg$elements[[search_name]],
                                         error = \(e) search_cfg$elements[[tolower(resource)]]))
    } else {
      stop("FHIR 'design' not provided, but 'resource' was not provided and could not be read from 'bundles' or ",
           "'search_name'. Please provide 'resource' argument or FHIR 'design'.")
    }
  }
  x <- fhir_crack(bundles = bundles, design = design, sep = sep, brackets = brackets, rm_empty_cols = rm_empty_cols,
                  verbose = verbose, data.table = data.table, format = format, keep_attr = keep_attr, ncores = n_cores)
  attr(x, "search_name") <- search_name
  attr(x, "search_resource") <- resource
  # arguments <- as.list(environment())
  # do.call(fhir_crack, arguments)
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
  if (is.null(sep)) sep <- config$sep
  if (is.null(brackets)) brackets <- config$brackets

  if (!is.null(search_name)) {
    if (!is.null(fhir_dfs[[search_name]])) {
      cat(str_glue("Provided 'search_name'. FHIR DF '{search_name}' used, argument 'indexed_df' is ignored."), "\n")
      indexed_df <- fhir_dfs[[search_name]]
    }
  } else {
    search_name <- attr(indexed_df, "search_name")
  }

  if (!is_nonempty_df(indexed_df)) {
    warning("'indexed_df' is not a dataframe with at least 1 row. No melting done, object returned unmodified.")
    return(indexed_df)
  }

  if (is.null(search_name)) search_name <- deparse1(substitute(indexed_df))

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
  molten_df <- fhir_melt(indexed_data_frame = indexed_df, columns = cols, brackets = brackets, sep = sep,
                         id_name = id_name, all_columns = all_columns)
  attr(molten_df, "search_name") <- attr(indexed_df, "search_name")
  attr(molten_df, "search_resource") <- attr(indexed_df, "search_resource")
  return(molten_df)
}

# fhir_melt_w_cfg loop over all existing melt_groups
fhir_melt_loop_w_cfg <- function(indexed_df = NULL,
                                 col_group_list = NULL,
                                 brackets = NULL,
                                 sep = NULL,
                                 id_name_template = NULL,
                                 all_columns = TRUE,
                                 rm_indices = TRUE,
                                 config = cfg,
                                 search_name = NULL) {
  # config
  if (is.null(sep)) sep <- config$sep
  if (is.null(brackets)) brackets <- config$brackets

  if (!is.null(search_name) && !is.null(fhir_dfs[[search_name]])) {
    cat(str_glue("Provided 'search_name'. FHIR DF '{search_name}' used, argument 'indexed_df' is ignored."), "\n")
    indexed_df <- fhir_dfs[[search_name]]
  }

  if (!is_nonempty_df(indexed_df)) {
    warning("'indexed_df' is not a dataframe with at least 1 row. No melting done, object returned unmodified.")
    return(indexed_df)
  }

  if (is.null(col_group_list)) col_group_list <- find_melt_col_groups(indexed_df, brackets)
  molten_df <- indexed_df
  if (length(col_group_list) == 0) {
    warning(str_glue("Separator '{sep}' not found in DF{search_name_msg}, returning {rm_indices_msg}.",
                     search_name_msg = if (is_useful_string(search_name)) str_glue(" '{search_name}'") else "",
                     rm_indices_msg = if (rm_indices) "DF with only indices removed" else "unmodified DF"))
  } else {
    cat(str_glue("Found {length(col_group_list)} melt group(s) for FHIR DF '{search_name}': '",
                     paste0(names(col_group_list), collapse = "', '"),
                     "'. Melting sequentially."), "\n")
    for (melt_group in names(col_group_list)) {
      molten_df <- fhir_melt_w_cfg(indexed_df = molten_df,
                                   cols = col_group_list[melt_group],
                                   brackets = brackets,
                                   sep = sep,
                                   id_name = paste0("resource_id_", melt_group),
                                   all_columns = all_columns,
                                   search_name = NULL)
    }
  }
  if (rm_indices) molten_df <- fhir_rm_indices(molten_df, brackets = brackets)
  return(molten_df)
}

