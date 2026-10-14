# Copyright 2026 Observational Health Data Sciences and Informatics
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

# Minimal reproducer for the Windows DuckDB / Andromeda file-locking issue.
#
# Run from a fresh R session:
#   Rscript --vanilla extras/ReproduceAndromedaLocking.R
#
# Defaults: 3 worker processes, 20 loadAndromeda() calls per worker, one
# shared Andromeda temp directory, and a payload containing 1 million rows.
# Optional environment variables:
#   ANDROMEDA_LOCK_WORKERS     Number of worker processes (default: 3)
#   ANDROMEDA_LOCK_LOADS       Loads per worker (default: 20)
#   ANDROMEDA_LOCK_ROWS        Rows in the test database (default: 1000000)
#   ANDROMEDA_LOCK_TEMP_MODE   "shared" or "per-worker" (default: shared)
#   ANDROMEDA_LOCK_OUTPUT      Parent results directory
#                              (default: %TEMP%/andromeda-locking-repro)

readPositiveInteger <- function(name, default) {
  text <- Sys.getenv(name, unset = as.character(default))
  value <- suppressWarnings(as.integer(text))
  if (is.na(value) || value < 1L) {
    stop(name, " must be a positive integer; received '", text, "'.")
  }
  value
}

timestamp <- function(time = Sys.time()) {
  format(time, "%Y-%m-%dT%H:%M:%OS6%z")
}

main <- function() {
  requiredPackages <- c("Andromeda", "DBI", "duckdb")
  missingPackages <- requiredPackages[!vapply(
    requiredPackages,
    requireNamespace,
    quietly = TRUE,
    FUN.VALUE = logical(1)
  )]
  if (length(missingPackages) != 0L) {
    stop("Missing required package(s): ", paste(missingPackages, collapse = ", "))
  }

  workerCount <- readPositiveInteger("ANDROMEDA_LOCK_WORKERS", 3L)
  loadsPerWorker <- readPositiveInteger("ANDROMEDA_LOCK_LOADS", 20L)
  rowCount <- readPositiveInteger("ANDROMEDA_LOCK_ROWS", 1000000L)
  tempMode <- tolower(Sys.getenv("ANDROMEDA_LOCK_TEMP_MODE", unset = "shared"))
  if (!tempMode %in% c("shared", "per-worker")) {
    stop("ANDROMEDA_LOCK_TEMP_MODE must be 'shared' or 'per-worker'.")
  }

  defaultOutput <- Sys.getenv("TEMP", unset = tempdir())
  if (!nzchar(defaultOutput)) {
    defaultOutput <- tempdir()
  }
  outputRoot <- Sys.getenv(
    "ANDROMEDA_LOCK_OUTPUT",
    unset = file.path(defaultOutput, "andromeda-locking-repro")
  )
  runId <- paste0(format(Sys.time(), "%Y%m%d-%H%M%S"), "-pid", Sys.getpid())
  runFolder <- file.path(outputRoot, runId)
  setupTemp <- file.path(runFolder, "setup")
  workerTempRoot <- file.path(runFolder, "andromedaTemp")
  dir.create(setupTemp, recursive = TRUE, showWarnings = FALSE)
  dir.create(workerTempRoot, recursive = TRUE, showWarnings = FALSE)
  if (!dir.exists(setupTemp) || !dir.exists(workerTempRoot)) {
    stop("Could not create the run folders under ", runFolder)
  }

  runFolder <- normalizePath(runFolder, winslash = "/", mustWork = TRUE)
  setupTemp <- normalizePath(setupTemp, winslash = "/", mustWork = TRUE)
  workerTempRoot <- normalizePath(workerTempRoot, winslash = "/", mustWork = TRUE)
  archive <- file.path(runFolder, "payload.zip")
  eventsFile <- file.path(runFolder, "events.csv")
  summaryFile <- file.path(runFolder, "summary.txt")

  cat("Minimal DuckDB / Andromeda locking reproducer\n")
  cat("Artifacts:", runFolder, "\n")
  cat("R:", R.version.string, "\n")
  cat("OS:", paste(Sys.info()[c("sysname", "release", "version")], collapse = " "), "\n")
  for (packageName in requiredPackages) {
    cat(packageName, ":", as.character(utils::packageVersion(packageName)), "\n")
  }
  cat("Workers:", workerCount, "\n")
  cat("Loads per worker:", loadsPerWorker, "\n")
  cat("Payload rows:", rowCount, "\n")
  cat("Temp mode:", tempMode, "\n\n")

  # Build one real Andromeda archive without materializing the payload in R.
  # hash(i) makes the data less compressible and more visible to file scanners.
  options(
    andromedaTempFolder = setupTemp,
    andromedaThreads = 1L,
    warnDiskSpaceThreshold = 0
  )
  sourceAndromeda <- suppressMessages(
    Andromeda::andromeda(options = list(threads = 1L))
  )
  sourceIsOpen <- TRUE
  on.exit({
    if (sourceIsOpen) {
      try(Andromeda::close(sourceAndromeda), silent = TRUE)
    }
  }, add = TRUE)
  DBI::dbExecute(
    sourceAndromeda,
    sprintf(
      paste(
        "CREATE TABLE payload AS",
        "SELECT i::BIGINT AS id, hash(i)::UBIGINT AS value",
        "FROM range(%d) AS x(i)"
      ),
      rowCount
    )
  )
  suppressMessages(Andromeda::saveAndromeda(sourceAndromeda, archive))
  sourceIsOpen <- FALSE
  stopifnot(file.exists(archive))
  archive <- normalizePath(archive, winslash = "/", mustWork = TRUE)
  cat("Archive:", archive, "\n")
  cat("Archive size (bytes):", file.info(archive)$size, "\n\n")

  cluster <- parallel::makePSOCKcluster(
    workerCount,
    outfile = file.path(runFolder, "workers.log")
  )
  on.exit(try(parallel::stopCluster(cluster), silent = TRUE), add = TRUE)

  # Match the SCCS topology, where required packages are loaded before tasks.
  invisible(parallel::clusterEvalQ(cluster, {
    stopifnot(
      requireNamespace("Andromeda", quietly = TRUE),
      requireNamespace("DBI", quietly = TRUE)
    )
    NULL
  }))

  # Give every worker enough time to receive its job, then begin together.
  startAt <- Sys.time() + 3
  resultList <- parallel::clusterApply(
    cluster,
    seq_len(workerCount),
    function(workerId, archive, workerTempRoot, tempMode, loadsPerWorker, startAt) {
      workerTimestamp <- function(time = Sys.time()) {
        format(time, "%Y-%m-%dT%H:%M:%OS6%z")
      }
      workerFormatCondition <- function(condition) {
        call <- conditionCall(condition)
        paste(
          paste(class(condition), collapse = "/"),
          conditionMessage(condition),
          if (is.null(call)) "" else paste(deparse(call), collapse = " "),
          sep = " | "
        )
      }
      workerExtractPath <- function(text) {
        match <- regmatches(
          text,
          regexpr("[A-Za-z]:[/\\\\][^\"'\\r\\n]*\\.duckdb", text, perl = TRUE)
        )
        if (length(match) == 0L || !nzchar(match)) NA_character_ else match
      }

      pid <- Sys.getpid()
      workerTemp <- if (identical(tempMode, "shared")) {
        workerTempRoot
      } else {
        file.path(workerTempRoot, paste0("worker-", pid))
      }
      dir.create(workerTemp, recursive = TRUE, showWarnings = FALSE)
      workerTemp <- normalizePath(workerTemp, winslash = "/", mustWork = TRUE)
      options(
        andromedaTempFolder = workerTemp,
        andromedaThreads = 1L,
        warnDiskSpaceThreshold = 0
      )

      wait <- as.numeric(difftime(startAt, Sys.time(), units = "secs"))
      if (wait > 0) {
        Sys.sleep(wait)
      }

      rows <- vector("list", loadsPerWorker)
      for (iteration in seq_len(loadsPerWorker)) {
        started <- Sys.time()
        loaded <- NULL
        success <- FALSE
        databasePath <- NA_character_
        errorClass <- NA_character_
        errorMessage <- NA_character_
        errorCondition <- NA_character_

        tryCatch(
          {
            loaded <- suppressMessages(
              Andromeda::loadAndromeda(
                archive,
                options = list(threads = 1L)
              )
            )
            databasePath <- normalizePath(
              loaded@dbname,
              winslash = "/",
              mustWork = FALSE
            )
            count <- DBI::dbGetQuery(loaded, "SELECT count(*) AS n FROM payload")$n[[1]]
            if (count < 1) {
              stop("Loaded payload was unexpectedly empty.")
            }
            success <- TRUE
          },
          error = function(e) {
            errorClass <<- paste(class(e), collapse = "/")
            errorMessage <<- conditionMessage(e)
            errorCondition <<- workerFormatCondition(e)
            if (is.na(databasePath)) {
              databasePath <<- workerExtractPath(conditionMessage(e))
            }
          },
          finally = {
            if (!is.null(loaded)) {
              try(Andromeda::close(loaded), silent = TRUE)
            }
          }
        )

        lockSignature <- !success &&
          grepl("being used by another process", errorMessage, ignore.case = TRUE) &&
          grepl("cannot open file|rapi_startup", errorMessage, ignore.case = TRUE)
        rows[[iteration]] <- data.frame(
          workerId = workerId,
          pid = pid,
          iteration = iteration,
          success = success,
          expectedLockSignature = lockSignature,
          startTime = workerTimestamp(started),
          endTime = workerTimestamp(),
          elapsedSeconds = as.numeric(difftime(Sys.time(), started, units = "secs")),
          workerTemp = workerTemp,
          databasePath = databasePath,
          errorClass = errorClass,
          errorMessage = errorMessage,
          errorCondition = errorCondition,
          stringsAsFactors = FALSE
        )
      }
      do.call(rbind, rows)
    },
    archive = archive,
    workerTempRoot = workerTempRoot,
    tempMode = tempMode,
    loadsPerWorker = loadsPerWorker,
    startAt = startAt
  )
  parallel::stopCluster(cluster)
  cluster <- NULL

  events <- do.call(rbind, resultList)
  events <- events[order(events$workerId, events$iteration), ]
  utils::write.csv(events, eventsFile, row.names = FALSE, na = "")

  classification <- if (any(events$expectedLockSignature)) {
    "REPRODUCED"
  } else if (all(events$success)) {
    "NOT REPRODUCED"
  } else {
    "OTHER FAILURE"
  }
  orphanedFiles <- list.files(
    workerTempRoot,
    pattern = "\\.duckdb($|\\.)",
    recursive = TRUE,
    full.names = TRUE
  )
  resolvedPaths <- events$databasePath[
    !is.na(events$databasePath) & nzchar(events$databasePath)
  ]
  summary <- c(
    paste("classification:", classification),
    paste("completed:", timestamp()),
    paste("R:", R.version.string),
    paste("OS:", paste(Sys.info()[c("sysname", "release", "version")], collapse = " ")),
    vapply(
      requiredPackages,
      function(x) paste0(x, ": ", utils::packageVersion(x)),
      character(1)
    ),
    paste("workers:", workerCount),
    paste("loadsPerWorker:", loadsPerWorker),
    paste("payloadRows:", rowCount),
    paste("archiveBytes:", file.info(archive)$size),
    paste("tempMode:", tempMode),
    paste("successfulLoads:", sum(events$success)),
    paste("failedLoads:", sum(!events$success)),
    paste("lockSignatureFailures:", sum(events$expectedLockSignature)),
    paste("distinctDatabasePaths:", length(unique(resolvedPaths))),
    paste("duplicateDatabasePaths:", sum(duplicated(resolvedPaths))),
    paste("remainingDuckdbFiles:", length(orphanedFiles)),
    paste("DUCKDB_R_HOME:", Sys.getenv("DUCKDB_R_HOME", unset = "<unset>")),
    "",
    "Remaining DuckDB files:",
    if (length(orphanedFiles) == 0L) "<none>" else orphanedFiles,
    "",
    capture.output(utils::sessionInfo())
  )
  writeLines(summary, summaryFile)

  workerSummary <- do.call(rbind, lapply(split(events, events$workerId), function(x) {
    data.frame(
      workerId = x$workerId[[1]],
      pid = x$pid[[1]],
      loads = nrow(x),
      successes = sum(x$success),
      lockFailures = sum(x$expectedLockSignature),
      stringsAsFactors = FALSE
    )
  }))
  cat("Results by worker:\n")
  print(workerSummary, row.names = FALSE)
  if (any(!events$success)) {
    cat("\nFailed loads:\n")
    print(events[!events$success, c(
      "workerId", "pid", "iteration", "expectedLockSignature",
      "databasePath", "errorMessage"
    )], row.names = FALSE)
  }
  cat("\nCLASSIFICATION:", classification, "\n")
  cat("Successful loads:", sum(events$success), "of", nrow(events), "\n")
  cat("Distinct database paths:", length(unique(resolvedPaths)), "\n")
  cat("Duplicate database paths:", sum(duplicated(resolvedPaths)), "\n")
  cat("Remaining DuckDB files:", length(orphanedFiles), "\n")
  cat("Artifacts:", runFolder, "\n")
  invisible(classification)
}

main()
