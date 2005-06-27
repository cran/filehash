\name{filehash-class}
\docType{class}
\alias{filehash-class}
\alias{dbFetch}
\alias{dbInsert}
\alias{dbExists}
\alias{dbList}
\alias{dbDelete}
\alias{dbReorganize}
\alias{dbDelete,filehash,character-method}
\alias{dbExists,filehash,character-method}
\alias{dbFetch,filehash,character-method}
\alias{dbInsert,filehash,character-method}
\alias{dbList,filehash-method}
\alias{dbReorganize,filehash-method}
\alias{db2env,filehash-method}

\title{Class "filehash"}

\description{
  These functions form the interface for a simple file-based hash table
  not unlike \code{gdbm} (although \emph{simpler}).
}

\section{Objects from the Class}{
  Objects can be created by calls of the form \code{new("filehash", ...)}.
}

\section{Slots}{
  \describe{
    \item{\code{datafile}:}{Object of class \code{"character"}, full
      path to the database file.}
    \item{\code{mapfile}:}{Object of class \code{"character"}, full path
    to the index file.}
    \item{\code{name}:}{Object of class \code{"character"}, name of the
      database.}
  }
}

\section{Methods}{
  \describe{
    \item{dbDelete}{\code{signature(db = "filehash", key =
	"character")}: The \code{dbDelete} function is for deleting
      elements, but all it does is remove the key from the lookup table.
      The actual data are still in the database (but inaccessible).  If
      you reinsert data for the same key, the new data are simply
      appended on to the end of the file.  Therefore, it's possible to
      have multiple copies of data lying around after a while,
      potentially making the database file big.}
    \item{dbExists}{\code{signature(db = "filehash", key =
	"character")}: check to see if a key exists.}
    \item{dbFetch}{\code{signature(db = "filehash", key = "character")}:
      retrieve the value associated with a given key.}
    \item{dbInsert}{\code{signature(db = "filehash", key =
	"character")}: insert a key-value pair into the database.  If
      that key already exists, its associated value is overwritten.}
    \item{dbList}{\code{signature(db = "filehash")}: list all keys in
      the database.}
    \item{dbReorganize}{\code{signature(db = "filehash")}: The
      \code{dbReorganize} function is there for the purpose of rewriting
      the database to remove all of the stale entries.  Basically, this
      function creates a new copy of the database and then overwrites
      the old copy.  This function has not been tested much and so
      should be considered \emph{experimental}.}
    \item{db2env}{\code{signature(db = "filehash")}: loads
      \code{"filehash"} database into an environment via calls to
      \code{makeActiveBinding}}
  }
}

\author{Roger D. Peng \email{rpeng@jhsph.edu}}


\examples{
dbCreate("myDB")  ## Create files 'myDB.idx' and 'myDB.rdb'
db <- dbInitialize("myDB")
dbInsert(db, "a", 1:10)
dbInsert(db, "b", rnorm(1000))
dbExists(db, "b")  ## 'TRUE'

dbList(db)  ## c("a", "b")
dbDelete(db, "a")
dbList(db) ## "a"
}
\keyword{classes}
