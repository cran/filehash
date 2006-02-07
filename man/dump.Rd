\name{dumpObjects}
\alias{dumpObjects}
\alias{dumpImage}
\alias{dumpDF}

\title{Dump objects of database}
\description{
  Dump R objects to a filehash database
}
\usage{
dumpObjects(..., list = character(0), dbName, envir = parent.frame())
dumpImage(dbName = "Rworkspace")
dumpDF(data, dbName = NULL)
}

\arguments{
  \item{\dots}{R objects to dump}
  \item{list}{character vector of names of objects to dump}
  \item{dbName}{character, name of database to which objects should be dumped}
  \item{envir}{environment from which to obtain objects}
  \item{data}{a data frame}
}
\details{
  Objects dumped to a database can later be loaded via \code{dbLoad}.
  Alternatively, the \code{with} method can be used to evaluate code in
  the context of a database.  If a database with name \code{dbName}
  already exists, objects will be inserted into the existing database
  (and values for already-existing keys will be overwritten).

  \code{dumpDF} is different in that each variable in the data frame is
  stored as a separate object in the database.  So each variable can be
  read from the database separately rather than having to load the
  entire data frame into memory.
}
\value{
  An object of class \code{"filehash"} is returned.
}

\author{Roger D. Peng}

\examples{
data <- data.frame(y = rnorm(100), x = rnorm(100), z = rnorm(100))
db <- dumpDF(data, dbName = "dataframe.dump")
fit <- with(db, lm(y ~ x + z))
summary(fit)
}
\keyword{database}% at least one, from doc/KEYWORDS

