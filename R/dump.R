dumpImage <- function(dbName = "Rworkspace") {
    dumpObjects(list = ls(envir = globalenv(), all.names = TRUE),
                dbName = dbName, envir = globalenv())
}

dumpObjects <- function(..., list = character(0), dbName, envir = parent.frame()) {
    names <- as.character(substitute(list(...)))[-1]
    list <- c(list, names)
    if(!dbCreate(dbName))
        stop("could not create database file")
    db <- dbInitialize(dbName)  ## use default DB type

    for(i in seq(along = list)) 
        dbInsert(db, list[i], get(list[i], envir))
    db
}

dumpDF <- function(data, dbName = NULL) {
    if(is.null(dbName))
        dbName <- as.character(substitute(data))
    if(!dbCreate(dbName))
        stop("could not create database file")
    db <- dbInitialize(dbName)
    vnames <- names(data)
    for(i in seq(along = vnames))
        dbInsert(db, vnames[i], data[[vnames[i]]])
    db
}
