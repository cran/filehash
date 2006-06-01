dumpImage <- function(dbName = "Rworkspace") {
    dumpObjects(list = ls(envir = globalenv(), all.names = TRUE),
                dbName = dbName, envir = globalenv())
}

dumpObjects <- function(..., list = character(0), dbName, envir = parent.frame()) {
    names <- as.character(substitute(list(...)))[-1]
    list <- c(list, names)
    if(!dbCreate(dbName, "DB1"))
        stop("could not create database file")
    db <- dbInitialize(dbName, filehashOption()$defaultType)

    for(i in seq(along = list)) 
        dbInsert(db, list[i], get(list[i], envir))
    db
}

dumpDF <- function(data, dbName = NULL) {
    if(is.null(dbName))
        dbName <- as.character(substitute(data))
    dumpList(as.list(data), dbName = dbName)
}

dumpList <- function(data, dbName = NULL) {
    if(!is.list(data))
        stop("'data' must be a list")
    vnames <- names(data)
    
    if(is.null(vnames) || isTRUE("" %in% vnames))
        stop("list must have non-empty names")
    if(is.null(dbName))
        dbName <- as.character(substitute(data))
    type <- filehashOption()$defaultType
    
    if(!dbCreate(dbName, type))
        stop("could not create database file")
    db <- dbInitialize(dbName, type)

    for(i in seq(along = vnames))
        dbInsert(db, vnames[i], data[[vnames[i]]])
    db
}

