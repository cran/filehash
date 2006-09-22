.onLoad <- function(lib, pkg) {
    if(!require(methods, quietly = TRUE))
        stop("'methods' package required")
    assign("defaultType", "DB1", .filehashOptions)

    for(type in c("DB1", "DB", "RDS")) {
        cname <- paste("create", type, sep = "")
        iname <- paste("initialize", type, sep = "")
        r <- list(create = get(cname, mode = "function"),
                  initialize = get(iname, mode="function"))
        assign(type, r, .filehashFormats)
    }
}

.onAttach <- function(lib, pkg) {
    dcf <- read.dcf(file.path(lib, pkg, "DESCRIPTION"))
    msg <- gettextf("%s (version %s %s)", dcf[, "Title"],
                    as.character(dcf[, "Version"]), dcf[, "Date"])
    writeLines(strwrap(msg))
}

.filehashOptions <- new.env()

.filehashFormats <- new.env()
