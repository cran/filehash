######################################################################
## Class 'filehash'

setClass("filehash", representation(name = "character"))

setGeneric("dbName", function(db) standardGeneric("dbName"))
setMethod("dbName", "filehash", function(db) db@name)

setMethod("show", "filehash",
          function(object) {
              cat("'filehash' database", sQuote(object@name), "\n")
          })


######################################################################

registerFormatDB <- function(name, funlist) {
    if(!all(c("initialize", "create") %in% names(funlist)))
        stop("need both 'initialize' and 'create' functions")
    r <- list(list(create = funlist[["create"]],
                   initialize = funlist[["initialize"]]))
    names(r) <- name
    do.call("filehashFormats", r)
    TRUE
}

filehashFormats <- function(...) {
    args <- list(...)
    n <- names(args)

    for(n in names(args)) 
        assign(n, args[[n]], .filehashFormats)
    current <- as.list(.filehashFormats)

    if(length(args) == 0)
        current
    else
    invisible(current)
}

######################################################################

createEmptyFile <- function(name) {
    ## If the file already exists, it is overwritten
    con <- file(name, "wb")
    close(con)
}

## Create necessary database files.  On successful creation, return
## TRUE.  If the database already exists, don't do anything but return
## TRUE (and print a message).  If there's any other strange
## condition, return FALSE.

dbStartup <- function(dbName, type, action = c("initialize", "create")) {
    action <- match.arg(action)
    validFormat <- type %in% names(filehashFormats())
    
    if(!validFormat) 
        stop(sQuote(type), " not a valid database format")
    formatList <- filehashFormats()[[type]]
    doFUN <- formatList[[action]]

    if(!is.function(doFUN))
        stop(sQuote(action), " function for database format ", sQuote(type),
             " is not valid")
    doFUN(dbName)
}    

    
dbCreate <- function(dbName, type) {
    if(missing(type))
        type <- filehashOption()$defaultType

    dbStartup(dbName, type, "create")
    TRUE
}

dbInit <- dbInitialize <- function(dbName, type) {
    if(missing(type))
        type <- filehashOption()$defaultType
    dbStartup(dbName, type, "initialize")
}

######################################################################
## Set options and retrieve list of options

filehashOption <- function(...) {
    args <- list(...)
    n <- names(args)

    for(n in names(args)) 
        assign(n, args[[n]], .filehashOptions)
    current <- as.list(.filehashOptions)

    if(length(args) == 0)
        current
    else
        invisible(current)
}

######################################################################
## Load active bindings into an environment

dbLoad <- function(db, env = parent.frame(), keys = NULL) {
    if(is.character(db))
        db <- dbInitialize(db)  ## use the default DB type
    if(is.null(keys))
        keys <- dbList(db)
    active <- sapply(keys, function(k) {
        exists(k, env, inherits = FALSE)
    })
    if(any(active)) {
        warning("keys with active/regular bindings ignored: ",
                paste(sQuote(keys[active]), collapse = ", "))
        keys <- keys[!active]
    }                      
    make.f <- function(k) {
        key <- k
        function(value) {
            if(!missing(value)) {
                dbInsert(db, key, value)
                invisible(value)
            }
            else {
                obj <- dbFetch(db, key)
                obj
            }
        }
    }
    for(k in keys) 
        makeActiveBinding(k, make.f(k), env)
    invisible(keys)
}

## Load active bindings into an environment and return the environment

db2env <- function(db) {
    if(is.character(db))
        db <- dbInitialize(db)  ## use the default DB type
    env <- new.env(hash = TRUE)
    dbLoad(db, env)
    env
}

######################################################################
## Other methods

setGeneric("with")
setMethod("with", "filehash",
          function(data, expr, ...) {
              env <- db2env(data)
              eval(substitute(expr), env, enclos = parent.frame())
          })

setGeneric("lapply")
setMethod("lapply", signature(X = "filehash"),
          function(X, FUN, ..., keep.names = TRUE) {
              FUN <- match.fun(FUN)
              keys <- dbList(X)
              rval <- vector("list", length = length(keys))
              
              for(i in seq(along = keys)) {
                  obj <- dbFetch(X, keys[i])
                  rval[[i]] <- FUN(obj, ...)
              }
              if(keep.names)
                  names(rval) <- keys
              rval
          })

## setGeneric("names")
## setMethod("names", signature(x = "filehash"),
##           function(x) {
## 
##           })

######################################################################
## Database interface

setGeneric("getMap", function(db) standardGeneric("getMap"))

setGeneric("dbInsert", function(db, key, value) standardGeneric("dbInsert"))
setGeneric("dbFetch", function(db, key) standardGeneric("dbFetch"))
setGeneric("dbExists", function(db, key) standardGeneric("dbExists"))
setGeneric("dbList", function(db) standardGeneric("dbList"))
setGeneric("dbDelete", function(db, key) standardGeneric("dbDelete"))
setGeneric("dbReorganize", function(db) standardGeneric("dbReorganize"))
setGeneric("dbUnlink", function(db) standardGeneric("dbUnlink"))
setGeneric("dbDisconnect", function(db) standardGeneric("dbDisconnect"))

######################################################################
## Extractor/replacement

setMethod("[[", signature(x = "filehash", i = "character", j = "missing"),
          function(x, i, j) {
              dbFetch(x, i)
          })

setMethod("$", signature(x = "filehash", name = "character"),
          function(x, name) {
              dbFetch(x, name)
          })

setReplaceMethod("[[", signature(x = "filehash", i = "character", j = "missing"),
                 function(x, i, j, value) {
                     dbInsert(x, i, value)
                     x
                 })

setReplaceMethod("$", signature(x = "filehash", name = "character"),
                 function(x, name, value) {
                     dbInsert(x, name, value)
                     x
                 })


## Need to define these because they're not automatically caught

setReplaceMethod("[[", signature(x = "filehash", i = "numeric", j = "missing"),
                 function(x, i, j, value) {
                     stop("use of numeric indices not allowed")
                 })

setMethod("[[", signature(x = "filehash", i = "numeric", j = "missing"),
          function(x, i, j) {
              stop("use of numeric indices not allowed")
          })

setMethod("[", signature(x = "filehash", i = "ANY", j = "ANY", drop = "missing"),
          function(x, i, j) {
              stop("use of non-character indices via '[' not allowed")
          })



######################################################################
## Miscellaneous


## 'serialize()' changed from 2.3.0 to 2.4.0 so we need this function
## for back compatibility
toBytes <- function(x) {
    if(getRversion() < package_version("2.4.0"))
        charToRaw(serialize(x, NULL))
    else
        serialize(x, NULL)
}


