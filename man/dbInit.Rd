\name{dbInit}
\alias{dbInit}
\alias{dbCreate}
\alias{dbInitialize}

\title{Simple file-based hash table}
\description{
  Interface for a simple file-based hash table
}
\usage{
dbCreate(dbName, type = c("DB", "RDS"))
dbInitialize(dbName, type = c("DB", "RDS"))
}

\arguments{
  \item{dbName}{character, name of database}
  \item{type}{type of format}
}

\details{
  \code{dbCreate} creates the necessary files for the database and
  returns \code{TRUE} upon success.  If those files already exist, they
  are overwritten with a warning.

  \code{dbInitialize} takes a database name and returns an object of
  class \code{"filehash"}.

  The \code{type} argument specifies how the database should be stored
  on the disk, either as a single file (\code{"DB"}) or as a collection of
  files in a directory (\code{"RDS"}).
}

\author{Roger D. Peng}

\seealso{
  See \code{\link{filehash-class}} more information and examples.
}

\keyword{database}% at least one, from doc/KEYWORDS

