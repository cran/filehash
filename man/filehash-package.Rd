\name{filehash-package}
\alias{filehash-package}
\alias{filehash}
\docType{package}
\title{
filehash overview
}
\description{
Simple file-based hash table
}
\details{
\tabular{ll}{
Package: \tab filehash\cr
Version: \tab 0.2\cr
Date: \tab 2005-06-25\cr
LazyLoad: \tab yes\cr
Depends: \tab R (>= 2.1.0)\cr
License: \tab GNU GPL (version 2 or later)\cr
URL: \tab http://sandybox.typepad.com/software/\cr
}

Index:
\preformatted{
  db2env                  Load database into environment
  dbUtils                 Simple file-based hash table
  filehash-class          Class "filehash"
  filehash-package        filehash overview
}

This package implements a very simple file-based hash table
(i.e. key-value database).  The idea is basically the same as that
used by R internally for its lazy-loaded databases but the
implementation is far cruder.  There is no C code here, just pure R.

One advantage of using a GDBM-type system is that it puts all the data
into one file rather than having many files lying around.  One
disadvantage of using GDBM specifically is that the database file is
not portable.  For example, you need separate versions of the same
database for i686 and x86\_64 architectures.

\code{filehash} takes objects, serializes them to an ASCII string and
converts the string to raw bytes (via \code{charToRaw}).  The raw sequence
of bytes is appended to a database file and the starting and ending
offset positions in the file are stored in an environment.  When a
user requests an object associated with a specific key via \code{dbFetch},
the key is used to look up the byte/offset numbers and then the data
are read from that position in the database file.  The \code{filehash}
package uses 2 files---one file (with extension \code{idx}) to store the
index positions and another file (with extension \code{rdb}) to store
the data.
}

\author{
Roger D. Peng <rpeng@jhsph.edu>
}

\keyword{database}

\examples{
## See 'class ? filehash' for examples
}
