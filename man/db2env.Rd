\name{db2env}
\alias{db2env}

\title{Load database into environment}
\description{
  Load entire database into an environment
}
\usage{
db2env(db)
}
\arguments{
  \item{db}{database object}
}
\details{
  \code{db2env} is generic but there is a method for objects of class
  \code{"filehash"}.
  
  \code{db2env} loads the entire database \code{db} into an environment
  via calls to \code{makeActiveBinding}.  Therefore, the data themselves
  are not stored in the environment, but a function pointing to the data
  in the database is stored.  When an element of the environment is
  accessed, the function is called to retrieve the data from the
  database.  If the data in the database is changed, the changes will be
  reflected in the environment.

  The use of \code{makeActiveBinding} allows for potentially large
  databases to, at least conceptually, be used in R, as long as you
  don't need simultaneous access to all of the elements in the
  database.  
}
\value{
  An environment is returned, the elements of which are the keys of the
  database.  
}

\author{Roger D. Peng}

\seealso{
  \code{\link{dbUtils}} and \code{\link{filehash-class}}
}

\examples{
dbCreate("myDB")
db <- dbInitialize("myDB")
dbInsert(db, "a", rnorm(100))
dbInsert(db, "b", 1:10)

env <- db2env(db)
ls(env)  ## "a", "b"
print(env$b)
mean(env$a)
env$a <- rnorm(100)
mean(env$a)

env$b[1:5] <- 5:1
print(env$b)
}

\keyword{database}
