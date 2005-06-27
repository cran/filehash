setClass("filehash",
         representation(datafile = "character",
                        mapfile = "character",
                        name = "character")
         )

setGeneric("dbName", function(db) standardGeneric("dbName"))

setMethod("dbName", "filehash", function(db) db@name)

setValidity("filehash",
            function(object) {
                if(!file.exists(object@datafile))
                    return(paste("datafile", object@datafile,
                                 "does not exist"))
                if(!file.exists(object@mapfile))
                    return(paste("mapfile", object@mapfile, "does not exist"))
                TRUE
            })

setGeneric("db2env", function(db) standardGeneric("db2env"))

setMethod("db2env", "filehash",
          function(db) {
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

setGeneric("getMap", function(db) standardGeneric("getMap"))

setMethod("getMap", "filehash", function(db) .readRDS(file = db@mapfile))

setGeneric("getDataCon", function(db) standardGeneric("getDataCon"))

setMethod("getDataCon", "filehash", function(db) file(db@datafile))


generateMapFile <- function(dbName) {
    stopifnot(length(dbName) == 1)
    paste(dbName, "idx", sep = ".")
}

generateDataFile <- function(dbName) {
    stopifnot(length(dbName) == 1)
    paste(dbName, "rdb", sep = ".")
}


dbCreate <- function(dbName) {
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
}

dbInitialize <- function(dbName) {
    new("filehash", datafile = generateDataFile(dbName),
        mapfile = generateMapFile(dbName), name = dbName)
}

setGeneric("dbInsert", function(db, key, value) standardGeneric("dbInsert"))

setMethod("dbInsert",
          signature(db = "filehash", key = "character", value = "ANY"),
          function(db, key, value) {
              map <- getMap(db)
              con <- getDataCon(db)
              map[[key]] <- appendData(con, value)
              .saveRDS(map, file = db@mapfile, compress = TRUE)
              TRUE
          }
          )

setGeneric("dbFetch", function(db, key) standardGeneric("dbFetch"))

setMethod("dbFetch", signature(db = "filehash", key = "character"),
          function(db, key) {
              map <- getMap(db)
              
              if(!dbExists(db, key))
                  stop(sQuote(key), " does not exist")
              idx <- map[[key]]
              con <- getDataCon(db)
              readData(con, idx[1], idx[2])
          }
          )

setGeneric("dbExists", function(db, key) standardGeneric("dbExists"))

setMethod("dbExists", signature(db = "filehash", key = "character"),
          function(db, key) {
              map <- getMap(db)
              exists(key, map, inherits = FALSE)
          }
          )

setGeneric("dbList", function(db) standardGeneric("dbList"))

setMethod("dbList", "filehash",
          function(db) {
              map <- getMap(db)
              ls(map)
          }
          )

setGeneric("dbDelete", function(db, key) standardGeneric("dbDelete"))

setMethod("dbDelete", signature(db = "filehash", key = "character"),
          function(db, key) {
              map <- getMap(db)
              rm(list = key, pos = map)
              .saveRDS(map, file = db@mapfile, compress = TRUE)
              TRUE
          }
          )

setGeneric("dbReorganize", function(db) standardGeneric("dbReorganize"))

setMethod("dbReorganize", "filehash",
          function(db) {
              tempdir <- tempfile()
              dir.create(tempdir)
              tempdbName <- file.path(tempdir, basename(db))
              
              tempdb <- dbCreate(tempdbName)
              for(key in dbList(db)) {
                  dbInsert(tempdb, key, dbFetch(db, key))
              }
              file.copy(tempdb@mapfile, db@mapfile)
              file.copy(tempdb@datafile, db@datafile)
              TRUE
          }
          )



readData <- function(con, start, len) {
    if(!isOpen(con)) {
        open(con, "rb")
        on.exit(close(con))
    }
    stopifnot(isSeekable(con))
    seek(con, start)
    data <- readBin(con, "raw", len)
    unserialize(rawToChar(data))
}

appendData <- function(con, data) {
    stopifnot(isSeekable(con))

    if(!isOpen(con)) {
        open(con, "ab")
        on.exit(close(con))
    }
    start <- getEndPos(con)

    byteData <- charToRaw(serialize(data, NULL, ascii = TRUE))
    writeBin(byteData, con)

    c(start, length(byteData))
}

getEndPos <- function(con) {
    seek(con, 0, "end")
    seek(con)
}


