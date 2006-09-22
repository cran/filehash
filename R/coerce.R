toDBType <- function(from, type, dbpath = NULL) {
    if(is.null(dbpath))
        dbpath <- dbName(from)
    if(!dbCreate(dbpath, type = type))
        stop("could not create ", type, " database")
    db <- dbInit(dbpath, type = type)
    keys <- dbList(from)
    
    for(key in keys)
        dbInsert(db, key, dbFetch(from, key))
    invisible(db)
}

setAs("filehashDB1", "filehashRDS",
      function(from) {
          dbpath <- paste(dbName(from), "RDS", sep = "")
          toDBType(from, "RDS", dbpath)
      })
      
setAs("filehashDB", "filehashRDS",
      function(from) {
          toDBType(from, "RDS")
      })

setAs("filehashRDS", "filehashDB",
      function(from) {
          toDBType(from, "DB")
      })

setAs("filehashDB", "filehashDB1",
      function(from) {
          toDBType(from, "DB1")
      })

setAs("filehashDB1", "list",
      function(from) {
          keys <- dbList(from)
          dbMultiFetch(from, keys)
      })
