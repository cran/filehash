######################################################################
## Class 'filehashRDS'

setClass("filehashRDS",
         representation(dbDir = "character"),
         contains = "filehash"
         )

setValidity("filehashRDS",
            function(object) {
                if(length(object@dbDir) != 1)
                    return("only one directory should be set in 'dbDir'")
                if(!file.exists(object@dbDir))
                    return(gettextf("directory '%s' does not exist", object@dbDir))
                TRUE
            })

createRDS <- function(dbName) {
    dbDir <- dbName
    
    if(!file.exists(dbDir))
        dir.create(dbDir)
    else
        message(gettextf("database '%s' already exists", dbName))
    TRUE
}

initializeRDS <- function(dbName) {
    new("filehashRDS", dbDir = normalizePath(dbName), name = basename(dbName))
}

## For case-insensitive file systems, objects with the same name but
## differ by capitalization might get clobbered.  `mangleName()'
## inserts a "@" before each capital letter and `unMangleName()'
## reverses the operation.

mangleName <- function(oname) {
    gsub("([A-Z])", "@\\1", oname, perl = TRUE)
}

unMangleName <- function(mname) {
    gsub("@", "", mname, fixed = TRUE)
}

## Function for mapping a key to a path on the filesystem
setGeneric("objectFile", function(db, key) standardGeneric("objectFile"))
setMethod("objectFile", signature(db = "filehashRDS", key = "character"),
          function(db, key) {
              file.path(db@dbDir, mangleName(key))
          })

######################################################################
## Interface functions

setMethod("dbInsert",
          signature(db = "filehashRDS", key = "character", value = "ANY"),
          function(db, key, value, ...) {
              ## open connection to a gzip compressed file
              con <- gzfile(objectFile(db, key), "wb")

              ## serialize data to connection; return TRUE on success
              tryCatch({
                  serialize(value, con)
                  TRUE
              }, finally = close(con))
          }
          )

setMethod("dbFetch", signature(db = "filehashRDS", key = "character"),
          function(db, key, ...) {
              ## if(!dbExists(db, key))
              ##     stop(gettextf("'%s' not in database", key))

              ## create filename from key
              ofile <- objectFile(db, key)

              ## if(!file.exists(ofile))
              ##     stop(gettextf("'%s' not in database", key))

              ## open connection to file and unserialize object from
              ## connection
              con <- tryCatch({
                  gzfile(ofile, "rb")
              }, error = function(cond) {
                  cond
              })
              if(inherits(con, "condition")) 
                  stop(gettextf("error obtaining value for key '%s': %s", key,
                                conditionMessage(con)))
              on.exit(close(con))
              
              val <- unserialize(con)
              val
          })

setMethod("dbExists", signature(db = "filehashRDS", key = "character"),
          function(db, key, ...) {
              ## fileList <- dir(db@dbDir, all.files = TRUE)
              ## key %in% unMangleName(fileList)
              key %in% dbList(db)
          })

setMethod("dbList", "filehashRDS",
          function(db, ...) {
              ## list all keys/files in the database
              fileList <- dir(db@dbDir, all.files = TRUE, full.names = TRUE)
              use <- !file.info(fileList)$isdir
              fileList <- basename(fileList[use])

              ## fileList <- fileList[-match(c(".", ".."), fileList)]
              unMangleName(fileList)
          })

setMethod("dbDelete", signature(db = "filehashRDS", key = "character"),
          function(db, key, ...) {
              ofile <- objectFile(db, key)
              
              ## remove/delete the file
              status <- file.remove(ofile)
              isTRUE(status)
          })

setMethod("dbUnlink", "filehashRDS",
          function(db, ...) {
              ## delete the entire database directory
              d <- db@dbDir
              unlink(d, recursive = TRUE)
              TRUE
          })

