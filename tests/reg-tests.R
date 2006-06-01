library(filehash)

######################################################################
## Test 'filehashDB' class
## Create a simple database

dbCreate("mydb", "DB")
db <- dbInitialize("mydb", "DB")

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

numbers <- rnorm(100)
dbInsert(db, "numbers", numbers)
b <- dbFetch(db, "numbers")
stopifnot(all.equal(numbers, b))
stopifnot(identical(numbers, b))

######################################################################
## Test 'filehashRDS' class

dbCreate("mydbRDS", "RDS")
db <- dbInitialize("mydbRDS", "RDS")
show(db)

## Put some data into it
set.seed(1000)
dbInsert(db, "a", 1:10)
dbInsert(db, "b", rnorm(100))
dbInsert(db, "c", 100:1)
dbInsert(db, "d", runif(1000))
dbInsert(db, "other", "hello")

dbList(db)

dbExists(db, "e")
dbExists(db, "a")

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


######################################################################
## test filehashDB1 class

dbCreate("mydb", "DB1")
db <- dbInitialize("mydb", "DB1")

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

numbers <- rnorm(100)
dbInsert(db, "numbers", numbers)
b <- dbFetch(db, "numbers")
stopifnot(all.equal(numbers, b))
stopifnot(identical(numbers, b))
