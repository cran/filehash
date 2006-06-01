######################################################################
## Class 'filehashRDS'

createRDS <- function(dbName) {
    dbDir <- dbName
    
    if(!file.exists(dbDir))
        dir.create(dbDir)
    else
        message("database ", sQuote(dbName), " already exists")
    TRUE
}

initializeRDS <- function(dbName) {
    new("filehashRDS", dbDir = dbName, name = basename(dbName))
}

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

setGeneric("objectFile", function(db, key) standardGeneric("objectFile"))
setMethod("objectFile", signature(db = "filehashRDS", key = "character"),
          function(db, key) {
              file.path(db@dbDir, mangleName(key))
          })

setMethod("dbInsert",
          signature(db = "filehashRDS", key = "character", value = "ANY"),
          function(db, key, value) {
              con <- gzfile(objectFile(db, key), "wb")
              
              tryCatch({
                  serialize(value, con)
                  TRUE
              }, finally = close(con))
          }
          )

setMethod("dbFetch", signature(db = "filehashRDS", key = "character"),
          function(db, key) {
              if(!dbExists(db, key))
                  stop(sQuote(key), " not in database")
              ofile <- objectFile(db, key)

              if(!file.exists(ofile))
                  stop(sQuote(key), " not in database")
              con <- gzfile(ofile, "rb")

              tryCatch({
                  obj <- unserialize(con)
                  obj
              }, condition = function(cond) {
                  NULL
              }, finally = close(con))
          })

setMethod("dbExists", signature(db = "filehashRDS", key = "character"),
          function(db, key) {
              fileList <- dir(db@dbDir, all.files = TRUE)
              key %in% unMangleName(fileList)
          })

setMethod("dbList", "filehashRDS",
          function(db) {
              fileList <- dir(db@dbDir, all.files = TRUE)
              fileList <- fileList[-match(c(".", ".."), fileList)]
              unMangleName(fileList)
          })

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

setMethod("dbUnlink", "filehashRDS",
          function(db) {
              d <- db@dbDir
              unlink(d, recursive = TRUE)
              TRUE
          })

