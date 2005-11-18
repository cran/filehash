.onAttach <- function(lib, pkg) {
    dcf <- read.dcf(file.path(lib, pkg, "DESCRIPTION"))
    msg <- paste(dcf[, "Title"], " (version ",
                 as.character(dcf[, "Version"]), ")", sep = "")
    writeLines(strwrap(msg))
}
