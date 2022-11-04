#!/usr/bin/env Rscript

# CoverCHILD data ETL
# @author: SP
# @date: 2022-11-04

library("tidyverse")
library("magrittr")
library("tools")

files <- Sys.glob("data/*.csv")
names(files) <- basename(file_path_sans_ext(files))
data_raw <- lapply(files, \(x) {
  encoding <- try(guess_encoding(x)$encoding[[1]])
  if (inherits(encoding, "try-error")) encoding <- "ISO-8859-1"
  read_csv2(x, locale = locale("de", encoding = encoding))
  })

# convert to DFs in global env if desired
# for (x in names(data_raw)) assign(x, data_raw[[x]])

# save UTF-8 encoded
outdir <- file.path("output", "data_utf8")
dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
for (x in names(data_raw)) write_csv2(data_raw[[x]], file.path(outdir, str_c(x, "_utf8.csv")))
