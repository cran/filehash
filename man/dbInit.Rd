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
  \item{type}{type of database format.  If missing, the default type
    will be used.}
}

\details{
  \code{dbCreate} creates the necessary files or directory for the
  database.  If those files already exist nothing is done.

  \code{dbInitialize} takes a database name and returns an object
  inheriting from class \code{"filehash"}.

  The \code{type} argument specifies how the database should be stored
  on the disk, either as a pair of files (\code{"DB"}) or as a
    made to automatically determine the typecollection of
  files in a directory (\code{"RDS"}).  If not specified, the default
  type will be used (as specified by \code{filehashOption}).  
}

\value{
  \code{dbCreate} returns \code{TRUE} upon success and \code{FALSE} in
  the event of an error.
}

\author{Roger D. Peng}

\seealso{
  See \code{\link{filehash-class}} more information and examples and
  \code{\link{filehashOption}} for setting the default database type.
}

\keyword{database}% at least one, from doc/KEYWORDS

