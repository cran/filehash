library(filehash)

## DB format
dbCreate("testDB", "DB")
db <- dbInitialize("testDB", "DB")

set.seed(10)
dbInsert(db, "integer", 1:10)
dbInsert(db, "numeric", rnorm(100))
dbInsert(db, "logical", c(TRUE, FALSE))
dbInsert(db, "char", letters)

library(datasets)
dbInsert(db, "dataframe", airquality)
dbInsert(db, "list", list(1, 2, 3, 4, 5))


## RDS format
dbCreate("testRDS", "RDS")
db <- dbInitialize("testRDS", "RDS")

set.seed(10)
dbInsert(db, "integer", 1:10)
dbInsert(db, "numeric", rnorm(100))
dbInsert(db, "logical", c(TRUE, FALSE))
dbInsert(db, "char", letters)

library(datasets)
dbInsert(db, "dataframe", airquality)
dbInsert(db, "list", list(1, 2, 3, 4, 5))
dbInsert(db, "Capital", LETTERS)
