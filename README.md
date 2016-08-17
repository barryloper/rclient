# RClient

A pooled wrapper for pyRserve.

## RServe Setup (OSX)

First, install the latest version of the [R project](https://www.r-project.org/).
 
Homebrew users can get this with `brew install r`
 
RServe requires gettext and opnssl in order to compile. 

Install with `brew install gettext openssl`.

RServe must know where to find these homebrew versions of gettext and openssl. I got it to compile with the following command.

`LIBRARY_PATH="/usr/local/opt/openssl/lib:/usr/local/opt/gettext/lib" C_INCLUDE_PATH="/usr/local/opt/openssl/include:/usr/local/opt/gettext/include" R -e "install.packages('Rserve', repos='http://rforge.net')"`

## Starting the R Server

`rserve_init.sh` is designed to set up an RServe process for testing purposes.

Run `./rserve_init.sh start` to start an RServe process in the background.

Run `./rserve_init.sh stop` to end the daemon.

## Python demo

### Pooled R code evaluation
```Python3

from rclient import RConnection

r = RConnection(pool_size=5, realtime=False)

result = r.eval("2+2")

```

### Interactive R code evaluation

```Python3

from rclient import RConnection

r = RConnection(pool_size=5, realtime=False)

c1 = r.connect()

c1.voidEval("f1 <- function(){2+2}")
result = c1.eval("f1()")

c1.close()
```

### Context-managed evaluation

```Python3

from rclient import RConnection

r = RConnection(pool_size=5, realtime=False)

with r.connect() as c1:

    c1.voidEval("f1 <- function(){2+2}")
    result = c1.eval("f1()")

```


## API

Interactive connections created via `RConnection::connect` proxy pyRserve connection objects, and so follow mostly the same API.

[pyRserve documentation](https://pythonhosted.org/pyRserve/manual.html)

Only the `shutdown` method is overridden to prevent one connection from shutting down the whole RServe daemon.

`RConnection::eval` is a method that pulls a connection from the pool, evaluates some code, then returns the connection to the pool. This is useful for non-interactive work.


## Demo Notebook

`rpool.ipynb` is a notebook with some demo code, if you want to try it out.

