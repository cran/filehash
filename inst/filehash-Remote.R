######################################################################
## Class 'filehashRemote' for web-based repositories

setClass("reposInfo",
         representation(remoteURL = "character",
                        keys = "character",
                        name = "character")
         )

setClass("filehashRemote",
         representation(remoteURL = "character",
                        reposInfo = "reposInfo",
                        local = "filehash"),
         contains = "filehash")

setMethod("objectFile", signature(db = "filehashRemote", key = "character"),
          function(db, key) {
              paste(db@remoteURL, key, sep = "/")
          })

setGeneric("writeReposInfo", function(db, ...) {
    standardGeneric("writeReposInfo")
})

setMethod("writeReposInfo", "filehashRDS",
          function(db, remoteURL, file) {
              rinfo <- new("reposInfo", remoteURL = URLencode(remoteURL),
                           keys = dbList(db), name = dbName(db))
              con <- file(file, "w")
              on.exit(close(con))

              serialize(rinfo, con = con)
              invisible(rinfo)
          })

readReposInfo <- function(con) {
    if(!isOpen(con)) {
        open(con, "r")
        on.exit(close(con))
    }
    rinfo <- unserialize(con)
    validObject(rinfo)
    rinfo
}
