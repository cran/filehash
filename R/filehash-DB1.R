######################################################################
## Class 'filehashDB'

## NOTE: for 'numeric' data, we agree to read and write in little
## endian format.  I think things that are 'serialize()-ed' don't need
## to worry about endian-ness because it's encoded in the
## serialization process.

## Database entries
##
## File format: [key]        [nbytes data] [data]
##              serialized   serialized    raw bytes (serialized)
##

######################################################################

setOldClass("file")

## 'meta' is an empty list with an attributed called 'metaEnv'.
## 'metaEnv' is an environment that contains metadata for the
## database.

setClass("filehashDB1",
         representation(datafile = "character",
                        filecon = "file",
                        meta = "list"),  ## contains 'metaEnv' attribute
         contains = "filehash"
         )

setValidity("filehashDB1",
            function(object) {
                if(!file.exists(object@datafile))
                    return(paste("datafile", datafile, "does not exist"))
                TRUE
            })

createDB1 <- function(dbName) {
    if(!hasWorkingFtell())
        stop("need working 'ftell()' to use DB1 format")
    if(!file.exists(dbName))
        createEmptyFile(dbName)
    else
        message("database ", sQuote(dbName), " already exists")
    TRUE
}

initializeDB1 <- function(dbName) {
    if(!hasWorkingFtell())
        stop("need working 'ftell()' to use DB1 format")
    con <- file(dbName, "a+b")
    
    ## Create database map and store in environment.  Don't read map
    ## until you need it; for example, it's not needed for *writing*
    ## to the database.    
    metaEnv <- new.env(parent = emptyenv())
    assign("map", NULL, metaEnv)  ## 'NULL' indicates it needs to be read
    assign("dbfilesize", file.info(dbName)$size, metaEnv)

    ## This list stores the connection number for the file connection.
    ## Store the connection list in an environment and register a
    ## finalizer to close those connection when the environment is
    ## garbage collected.
    conList <- list(con = unclass(con))
    assign("conList", conList, metaEnv)

    reg.finalizer(metaEnv, function(env) {
        conList <- get("conList", env)
        for(i in seq(along = conList)) {
            con <- getConnection(conList[[i]])

            if(!is.null(con))
                close(con)
        }
    })
    new("filehashDB1", datafile = dbName,
        filecon = con,
        meta = structure(list(), metaEnv = metaEnv),
        name = basename(dbName)
        )
}


findEndPos <- function(con) {
    seek(con, 0, "end")
    seek(con)
}

readKeyMap <- function(con, map = NULL, pos = 0) {
    endpos <- findEndPos(con)

    if(is.null(map)) {
        map <- new.env(hash = TRUE, parent = emptyenv())
        pos <- 0
    }
    if(endpos <= 0)
        return(map)
    seek(con, pos, "start", "read")
    
    while(pos < endpos) {
        key <- unserialize(con)
        datalen <- unserialize(con)
        pos <- seek(con, rw = "read")  ## Update position
        
        if(datalen > 0) {
            ## Negative values of 'datalen' indicate deleted keys so only
            ## record positive 'datalen' values
            map[[key]] <- pos

            ## Fast forward to the next key
            seek(con, datalen, "current", "read")
            pos <- pos + datalen
        }
        else {
            ## Key is deleted; there is no data after it
            if(exists(key, map, inherits = FALSE))
               remove(list = key, pos = map)
        }        
    } 
   map
}

convertDB1 <- function(old, new) {
    dbCreate(new, "DB1")
    newdb <- dbInit(new, "DB1")

    con <- file(old, "rb")
    on.exit(close(con))

    endpos <- findEndPos(con)
    pos <- 0

    while(pos < endpos) {
        keylen <- readBin(con, "numeric", endian = "little")
        key <- rawToChar(readBin(con, "raw", keylen))
        datalen <- readBin(con, "numeric", endian = "little")
        value <- unserialize(con)

        ## cat(key, "\n")
        dbInsert(newdb, key, value)
        pos <- seek(con)
    }
    newdb
}

readSingleKey <- function(con, map, key) {
    start <- map[[key]]

    if(is.null(start))
        stop(sQuote(key), " not in database")
    tryCatch({
        seek(con, start, rw = "read")
        unserialize(con)
    }, condition = function(cond) {
        ## If there's any problem reading the data, just return NULL
        NULL
    })
}

readKeys <- function(con, map, keys) {
    r <- lapply(keys, function(key) readSingleKey(con, map, key))
    names(r) <- keys
    r
}

writeNullKeyValue <- function(con, key) {
    writestart <- findEndPos(con)
    
    tryCatch({
        writeKey(con, key)

        len <- -1
        serialize(len, con)
    }, interrupt = function(cond) {
        ## Rewind the file back to where writing began and truncate at
        ## that position
        seek(con, writestart, "start", "write")
        truncate(con)
    }, finally = flush(con))
}

writeKey <- function(con, key) {
    ## Write out key
    serialize(key, con)
}

writeKeyValue <- function(con, key, value) {
    writestart <- findEndPos(con)
    
    tryCatch({
        writeKey(con, key)
        
        ## Serialize data to raw bytes
        byteData <- toBytes(value)
    
        ## Write out length of data
        len <- length(byteData)
        serialize(len, con)

        ## Write out data
        writeBin(byteData, con)
    }, interrupt = function(cond) {
        ## Rewind the file back to where writing began and truncate at
        ## that position
        seek(con, writestart, "start", "write")
        truncate(con)
    }, finally = flush(con))
}

######################################################################
## Internal utilities

filesize <- findEndPos

setGeneric("checkMap", function(db) standardGeneric("checkMap"))

setMethod("checkMap", "filehashDB1",
          function(db) {
              old.size <- get("dbfilesize", attr(db@meta, "metaEnv"))
              ## cur.size <- file.info(db@datafile)$size
              cur.size <- filesize(db@filecon)
              size.change <- old.size != cur.size
              map.orig <- getMap(db)

              map <- if(is.null(map.orig))
                  readKeyMap(db@filecon)
              else if(size.change)
                  readKeyMap(db@filecon, map.orig, old.size)
              else
                  map.orig
              
              if(!identical(map, map.orig)) {
                  assign("map", map, attr(db@meta, "metaEnv"))
                  assign("dbfilesize", cur.size, attr(db@meta, "metaEnv"))
              }
              invisible(db)
          })

setMethod("getMap", "filehashDB1",
          function(db) {
              get("map", attr(db@meta, "metaEnv"))
          })

######################################################################
## Interface functions

setMethod("dbInsert",
          signature(db = "filehashDB1", key = "character", value = "ANY"),
          function(db, key, value) {
              writeKeyValue(db@filecon, key, value)
              TRUE
          })

setMethod("dbFetch",
          signature(db = "filehashDB1", key = "character"),
          function(db, key) {
              checkMap(db)
              map <- getMap(db)
              r <- readKeys(db@filecon, map, key[1])
              r[[1]]
          })

setGeneric("dbMultiFetch", function(db, keys) standardGeneric("dbMultiFetch"))

setMethod("dbMultiFetch",
          signature(db = "filehashDB1", keys = "character"),
          function(db, keys) {
              checkMap(db)
              map <- getMap(db)
              readKeys(db@filecon, map, keys)
          })

setMethod("[", signature(x = "filehashDB1", i = "character", j = "missing",
                         drop = "missing"),
          function(x, i , j) {
              dbMultiFetch(x, i)
          })

setMethod("dbExists", signature(db = "filehashDB1", key = "character"),
          function(db, key) {
              dbkeys <- dbList(db)
              key %in% dbkeys
          })

setMethod("dbList", "filehashDB1",
          function(db) {
              checkMap(db)
              map <- getMap(db)
              if(length(map) == 0)
                  character(0)
              else
                  names(as.list(map, all.names = TRUE))
          })

setMethod("dbDelete", signature(db = "filehashDB1", key = "character"),
          function(db, key) {
              writeNullKeyValue(db@filecon, key)
              TRUE
          })

setMethod("dbReorganize", "filehashDB1",
          function(db) {
              stop("'dbReorganize' not yet implemented")
          })

setMethod("dbUnlink", "filehashDB1",
          function(db) {
              unlink(db@datafile)
              TRUE
          })

setMethod("dbDisconnect", "filehashDB1",
          function(db) {
              close(db@filecon)
              TRUE
          })

setMethod("dbReorganize", "filehashDB1",
          function(db) {
              datafile <- db@datafile

              ## Find a temporary file name
              tempdata <- paste(datafile, "Tmp", sep = "")
              i <- 0
              while(file.exists(tempdata)) {
                  i <- i + 1
                  tempdata <- paste(datafile, "Tmp", i, sep = "")
              }
              if(!dbCreate(tempdata, type = "DB1")) {
                  warning("could not create temporary database")
                  return(FALSE)
              }
              on.exit(file.remove(tempdata))
              
              tempdb <- dbInitialize(tempdata, type = "DB1")
              keys <- dbList(db)

              ## Copy all keys to temporary database
              for(key in keys) 
                  dbInsert(tempdb, key, dbFetch(db, key))

              dbDisconnect(tempdb)
              dbDisconnect(db)
              status <- file.rename(tempdata, datafile)
              
              if(!isTRUE(status)) {
                  on.exit()
                  warning("temporary database could not be renamed and is left in ",
                          tempdata)
                  return(FALSE)
              }
              message("original database has been disconnected; ",
                      "reload with 'dbInitialize'")
              TRUE
          })


######################################################################
## Test system's ftell()

hasWorkingFtell <- function() {
    tfile <- tempfile()
    con <- file(tfile, "wb")

    tryCatch({
        bytes <- raw(10)
        begin <- seek(con)

        if(begin != 0)
            return(FALSE)
        writeBin(bytes, con)
        end <- seek(con)
        offset <- end - begin
        isTRUE(offset == 10)
    }, finally = {
        close(con)
        unlink(tfile)
    })
}

######################################################################

setGeneric("nextEntry", function(db, ...) standardGeneric("nextEntry"))

setMethod("nextEntry", "filehashDB1",
          function(db, ...) {
              key <- unserialize(db@filecon)
              datalen <- unserialize(db@filecon)
              unserialize(db@filecon)
          })

