library(filehash)

## Create a simple database

dbCreate("mydb")
db <- dbInitialize("mydb")

## Put some data into it
set.seed(1000)
dbInsert(db, "a", 1:10)
dbInsert(db, "b", rnorm(100))
dbInsert(db, "c", 100:1)
dbInsert(db, "d", runif(1000))
dbInsert(db, "other", "hello")

dbList(db)

env <- db2env(db)
ls(env)

env$a
env$b
env$c
str(env$d)
env$other

env$b <- rnorm(100)
mean(env$b)

env$a[1:5] <- 5:1
print(env$a)

dbDelete(db, "c")

tryCatch(print(env$c), error = function(e) print(e))
tryCatch(dbFetch(db, "c"), error = function(e) print(e))
