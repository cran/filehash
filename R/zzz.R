.onLoad <- function(lib, pkg) {
    if(!require(methods, quietly = TRUE))
        stop("'methods' package required")
    assign("defaultType", "DB", .filehashOptions)
}

.onAttach <- function(lib, pkg) {
    dcf <- read.dcf(file.path(lib, pkg, "DESCRIPTION"))
    msg <- paste(dcf[, "Title"], " (version ",
                 as.character(dcf[, "Version"]), ")", sep = "")
    writeLines(strwrap(msg))
}

.filehashOptions <- new.env()

