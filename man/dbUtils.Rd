\name{dbUtils}
\alias{dbUtils}
\alias{dbCreate}
\alias{dbInitialize}

\title{Simple file-based hash table}
\description{
  Interface for a simple file-based hash table
}
\usage{
dbCreate(dbName)
dbInitialize(dbName)
}

\arguments{
  \item{dbName}{character, name of database}
}

\details{
  \code{dbCreate} creates the necessary files for the database and
  returns \code{TRUE} upon success.  If those files already exist, they
  are overwritten with a warning.

  \code{dbInitialize} takes a database name and returns an object of
  class \code{"filehash"}.
}

\author{Roger D. Peng}

\seealso{
  See \code{\link{filehash-class}} more information and examples.
}

\keyword{database}% at least one, from doc/KEYWORDS

