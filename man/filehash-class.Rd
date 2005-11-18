\name{filehash-class}
\docType{class}
\alias{filehash-class}
\alias{filehashDB-class}
\alias{filehashRDS-class}
\alias{dbFetch}
\alias{dbInsert}
\alias{dbExists}
\alias{dbList}
\alias{dbDelete}
\alias{dbReorganize}
\alias{dbDelete,filehashDB,character-method}
\alias{dbExists,filehashDB,character-method}
\alias{dbFetch,filehashDB,character-method}
\alias{dbInsert,filehashDB,character-method}
\alias{dbList,filehashDB-method}
\alias{dbReorganize,filehashDB-method}
\alias{dbDelete,filehashRDS,character-method}
\alias{dbExists,filehashRDS,character-method}
\alias{dbFetch,filehashRDS,character-method}
\alias{dbInsert,filehashRDS,character-method}
\alias{dbList,filehashRDS-method}
\alias{show,filehash-method}

%%\alias{db2env,filehash-method}

\title{Class "filehash"}

\description{
  These functions form the interface for a simple file-based hash
  table.
}

\section{Objects from the Class}{
  Objects can be created by calls of the form \code{new("filehash", ...)}.
}

\section{Slots}{
  \describe{
    \item{\code{name}:}{Object of class \code{"character"}, name of the
      database.}
  }
}

\section{Additional slots for "filehashDB"}{
  \describe{
    \item{\code{datafile}:}{Object of class \code{"character"}, full
      path to the database file.}
    \item{\code{mapfile}:}{Object of class \code{"character"}, full path
      to the index file.}
  }
}

\section{Additional slots for "filehashRDS"}{
  \describe{
    \item{dbDir:}{Directory where files are stored.}
  }
}

\section{Methods}{
  \describe{
    \item{dbDelete}{\code{signature(db = "filehash", key =
	"character")}: The \code{dbDelete} function is for deleting
      elements, but for the \code{"DB"} format all it does is remove the
      key from the lookup table. 
      The actual data are still in the database (but inaccessible).  If
      you reinsert data for the same key, the new data are simply
      appended on to the end of the file.  Therefore, it's possible to
      have multiple copies of data lying around after a while,
      potentially making the database file big.  The \code{"RDS"} format
      does not have this problem.}
    \item{dbExists}{\code{signature(db = "filehash", key =
	"character")}: check to see if a key exists.}
    \item{dbFetch}{\code{signature(db = "filehash", key = "character")}:
      retrieve the value associated with a given key.}
    \item{dbInsert}{\code{signature(db = "filehash", key =
	"character")}: insert a key-value pair into the database.  If
      that key already exists, its associated value is overwritten.}
    \item{dbList}{\code{signature(db = "filehash")}: list all keys in
      the database.}
    \item{dbReorganize}{\code{signature(db = "filehashDB")}: The
      \code{dbReorganize} function is there for the purpose of rewriting
      the database to remove all of the stale entries.  Basically, this
      function creates a new copy of the database and then overwrites
      the old copy.  This function has not been tested much and so
      should be considered \emph{experimental}.  \code{dbReorganize} is
      not needed when using the \code{"RDS"} format.}
    \item{db2env}{\code{signature(db = "filehash")}: loads
      \code{"filehash"} database into an environment via calls to
      \code{makeActiveBinding}}
    \item{show}{print method}
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
