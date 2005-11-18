######################################################################
## Class 'filehash'

setClass("filehash", representation(name = "character"))

setGeneric("dbName", function(db) standardGeneric("dbName"))
setMethod("dbName", "filehash", function(db) db@name)

setMethod("show", "filehash",
          function(object) {
              cat(dQuote("filehash"), "database", sQuote(object@name), "\n")
          })

######################################################################
## Class 'filehashDB'

setClass("filehashDB",
         representation(datafile = "character",
                        mapfile = "character"),
         contains = "filehash"
         )

setValidity("filehashDB",
            function(object) {
                if(!file.exists(object@datafile))
                    return(paste("datafile", object@datafile,
                                 "does not exist"))
                if(!file.exists(object@mapfile))
                    return(paste("mapfile", object@mapfile, "does not exist"))
                TRUE
            })

generateMapFile <- function(dbName) {
    stopifnot(length(dbName) == 1)
    paste(dbName, "idx", sep = ".")
}

generateDataFile <- function(dbName) {
    stopifnot(length(dbName) == 1)
    paste(dbName, "rdb", sep = ".")
}

readData <- function(con, start, len) {
    stopifnot(isOpen(con))
    stopifnot(isSeekable(con))
    seek(con, start)
    data <- readBin(con, "raw", len)
    unserialize(rawToChar(data))
}

appendData <- function(con, data) {
    stopifnot(isOpen(con))
    stopifnot(isSeekable(con))

    start <- getEndPos(con)
    byteData <- charToRaw(serialize(data, NULL, ascii = TRUE))
    writeBin(byteData, con)

    c(start, length(byteData))
}

getEndPos <- function(con) {
    seek(con, 0, "end")
    seek(con)
}

setGeneric("getMap", function(db) standardGeneric("getMap"))
setMethod("getMap", "filehashDB", function(db) .readRDS(file = db@mapfile))

setGeneric("getDataCon", function(db, ...) standardGeneric("getDataCon"))
setMethod("getDataCon", "filehashDB", function(db) file(db@datafile))

######################################################################
## Class 'filehashRDS'

setClass("filehashRDS",
         representation(dbDir = "character"),
         contains = "filehash"
         )

setValidity("filehashRDS",
            function(object) {
                if(length(object@dbDir) != 1)
                    return("only 1 directory should be set in 'dbDir'")
                if(!file.exists(object@dbDir))
                    return(paste("directory", object@dbDir, "does not exist"))
                TRUE
            })

setGeneric("objectFile", function(db, key) standardGeneric("objectFile"))
setMethod("objectFile", signature(db = "filehashRDS", key = "character"),
          function(db, key) {
              file.path(db@dbDir, key)
          })

######################################################################
## Class 'filehashHTTP'

## setClass("filehashHTTP",
##          representation(remoteURL = "character", localDB = "filehashRDS"),
##          contains = "filehash")
## 
## setMethod("objectFile", signature(db = "filehashHTTP", key = "character"),
##           function(db, key) {
##               paste(db@remoteURL, key, sep = "/")
##           })

######################################################################

dbCreate <- function(dbName, type = c("DB", "RDS")) {
    type <- match.arg(type)

    switch(type, DB = {
        map <- new.env(hash = TRUE, parent = NULL)
        mapfile <- generateMapFile(dbName)
        datafile <- generateDataFile(dbName)
    
        if(file.exists(mapfile))
            warning("overwriting existing file ", sQuote(mapfile),
                    " with index file")
        .saveRDS(map, file = mapfile, compress = TRUE)
        if(file.exists(datafile))
            warning("overwriting existing file ", sQuote(datafile),
                    " with data file")
        con <- file(datafile, "wb")
        close(con)
        TRUE
    }, RDS = {
        dbDir <- dbName
        dir.create(dbDir)
    })
}

dbInitialize <- function(dbName, type = c("DB", "RDS")) {
    type <- match.arg(type)
    switch(type, DB = {
        new("filehashDB", datafile = generateDataFile(dbName),
            mapfile = generateMapFile(dbName), name = basename(dbName))
    }, RDS = {
        new("filehashRDS", dbDir = dbName, name = basename(dbName))
    })
}

setGeneric("db2env", function(db, ...) standardGeneric("db2env"))
setGeneric("dbLoad", function(db, env, ...) standardGeneric("dbLoad"))

setGeneric("dbInsert", function(db, key, value) standardGeneric("dbInsert"))
setGeneric("dbFetch", function(db, key) standardGeneric("dbFetch"))
setGeneric("dbExists", function(db, key) standardGeneric("dbExists"))
setGeneric("dbList", function(db) standardGeneric("dbList"))
setGeneric("dbDelete", function(db, key) standardGeneric("dbDelete"))
setGeneric("dbReorganize", function(db) standardGeneric("dbReorganize"))

######################################################################

setMethod("dbLoad", signature(db = "filehash", env = "environment"),
          function(db, env = parent.frame(n = 2), ...) {
              keys <- dbList(db)

              make.f <- function(k) {
                  key <- k
                  function(value) {
                      if(!missing(value))
                          dbInsert(db, key, value)
                      else
                          dbFetch(db, key)
                  }
              }
              for(k in keys) 
                  makeActiveBinding(k, make.f(k), env)
              invisible(keys)
          })

setMethod("db2env", "filehash",
          function(db, ...) {
              env <- new.env(hash = TRUE)
              keys <- dbList(db)

              make.f <- function(k) {
                  key <- k
                  function(value) {
                      if(!missing(value))
                          dbInsert(db, key, value)
                      else
                          dbFetch(db, key)
                  }
              }
              for(k in keys) 
                  makeActiveBinding(k, make.f(k), env)
              env
          })


######################################################################

setMethod("dbInsert",
          signature(db = "filehashDB", key = "character", value = "ANY"),
          function(db, key, value) {
              map <- getMap(db)
              con <- getDataCon(db)
              open(con, "ab")
              on.exit(close(con))
              
              map[[key]] <- appendData(con, value)
              .saveRDS(map, file = db@mapfile, compress = TRUE)
              TRUE
          }
          )

setMethod("dbInsert",
          signature(db = "filehashRDS", key = "character", value = "ANY"),
          function(db, key, value) {
              tryCatch({
                  .saveRDS(value, file = objectFile(db, key), compress = TRUE)
                  TRUE
              }, error = function(cond) {
                  cat("error saving object associated with ", sQuote(key),
                      ": ", as.character(cond), "\n", sep = "")
                  cond
              })
          }
          )

setMethod("dbFetch", signature(db = "filehashDB", key = "character"),
          function(db, key) {
              map <- getMap(db)
              
              if(!dbExists(db, key))
                  stop(sQuote(key), " does not exist")
              idx <- map[[key]]
              con <- getDataCon(db)

              tryCatch({
                  open(con, "rb")
                  readData(con, idx[1], idx[2])
              }, error = function(cond) {
                  cat("error reading object associated with ", sQuote(key), ": ",
                      as.character(cond), "\n", sep = "")
                  cond
              }, finally = {
                  if(isOpen(con))
                      close(con)
              })
          })

setMethod("dbFetch", signature(db = "filehashRDS", key = "character"),
          function(db, key) {
              if(!dbExists(db, key))
                  stop(sQuote(key), " does not exist")
              ofile <- objectFile(db, key)

              if(!file.exists(ofile))
                  stop("value associated with ", sQuote(key), " is not in database")
              tryCatch({
                  .readRDS(file = ofile)
              }, error = function(cond) {
                  cat("error reading value associated with ", sQuote(key), ": ",
                      as.character(cond), "\n", sep = "")
                  cond
              })                           
          })

setMethod("dbExists", signature(db = "filehashDB", key = "character"),
          function(db, key) {
              map <- getMap(db)
              exists(key, map, inherits = FALSE)
          }
          )

setMethod("dbExists", signature(db = "filehashRDS", key = "character"),
          function(db, key) {
              fileList <- dir(db@dbDir, all.files = TRUE)
              key %in% fileList
          })

setMethod("dbList", "filehashDB",
          function(db) {
              map <- getMap(db)
              ls(map)
          }
          )

setMethod("dbList", "filehashRDS",
          function(db) {
              fileList <- dir(db@dbDir, all.files = TRUE)
              fileList[-match(c(".", ".."), fileList)]
          })

setMethod("dbDelete", signature(db = "filehashDB", key = "character"),
          function(db, key) {
              map <- getMap(db)
              rm(list = key, pos = map)
              .saveRDS(map, file = db@mapfile, compress = TRUE)
              TRUE
          }
          )

setMethod("dbDelete", signature(db = "filehashRDS", key = "character"),
          function(db, key) {
              ofile <- objectFile(db, key)

              if(!file.exists(ofile)) {
                  warning("key ", sQuote(key), " not deleted")
                  FALSE
              }
              else {
                  status <- unlink(ofile)
                  isTRUE(status == 0)
              }
          })

setMethod("dbReorganize", "filehashDB",
          function(db) {
              rval <- TRUE
              tempdir <- tempfile()
              dir.create(tempdir)
              tempdbName <- file.path(tempdir, db@name)
              
              dbCreate(tempdbName, type = "DB")
              tempdb <- dbInitialize(tempdbName, type = "DB")
              
              for(key in dbList(db)) {
                  dbInsert(tempdb, key, dbFetch(db, key))
              }
              status <- file.copy(tempdb@mapfile, db@mapfile, overwrite = TRUE)
              if(!isTRUE(all(status))) {
                  warning("problem copying map file")
                  rval <- FALSE
              }
              status <- file.copy(tempdb@datafile, db@datafile, overwrite = TRUE)
              if(!isTRUE(all(status))) {
                  warning("problem copying data file")
                  rval <- FALSE
              }
              rval
          }
          )


