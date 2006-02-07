\name{dbLoad}
\alias{dbLoad}
\alias{db2env}

\title{Load database into environment}
\description{
  Load entire database into an environment
}
\usage{
db2env(db)
dbLoad(db, env = parent.frame(), keys = NULL)
}
\arguments{
  \item{db}{database object}
  \item{env}{an environment}
  \item{keys}{character vector of database keys to load}
  \item{...}{other arguments passed to methods}
}
\details{
  \code{db2env} loads the entire database \code{db} into an environment
  via calls to \code{makeActiveBinding}.  Therefore, the data themselves
  are not stored in the environment, but a function pointing to the data
  in the database is stored.  When an element of the environment is
  accessed, the function is called to retrieve the data from the
  database.  If the data in the database is changed, the changes will be
  reflected in the environment.

  \code{dbLoad}  loads objects in the database directly into the
  environment specified (like \code{load} does).  \code{dbLoad} takes a
  second argument \code{env}, which is an environment, and the default for
  \code{env} is \code{parent.frame()}. 

  The use of \code{makeActiveBinding} in \code{db2env} and \code{dbLoad}
  allows for potentially large databases to, at least conceptually, be
  used in R, as long as you don't need simultaneous access to all of the
  elements in the database.  
}

\value{
  For \code{db2env}, an environment is returned, the elements of which
  are the keys of the database.  For \code{dbLoad}, a character vector
  is returned (invisibly) containing the keys associated with the values
  loaded into the environment.
}

\author{Roger D. Peng}

\seealso{
  \code{\link{dbInitialize}} and \code{\link{filehash-class}}
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

env <- new.env()
dbLoad(db, env)
ls(env)
}

\keyword{database}
