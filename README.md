# DBAPI

[![Build Status](https://travis-ci.org/JuliaDB/DBAPI.jl.svg)](https://travis-ci.org/JuliaDB/DBAPI.jl) [![Build status](https://ci.appveyor.com/api/projects/status/xf6c75kxvtluiuit?svg=true)](https://ci.appveyor.com/project/iamed2/dbapi-jl)
 [![codecov.io](http://codecov.io/github/JuliaDB/DBAPI.jl/coverage.svg?branch=master)](http://codecov.io/github/JuliaDB/DBAPI.jl?branch=master)

This module defines an abstract interface for database drivers in Julia.

This module contains abstract types, abstract required functions (which throw
a `NotImplementedError` by default), and abstract optional functions (which
throw a `NotSupportedError` by default).

Database drivers should import this module, subtype its types, and create
methods for its functions which implement the signatures and behavioural
specifications contained within this module.

This interface is largely inspired by Python's DB API 2.0, which is in the
public domain: [PEP0249](https://www.python.org/dev/peps/pep-0249).


## Null Handling

All values of type `T` that could be `null` should be returned as 
`Nullable{T}`. 

## Running tests (version numbers shown may be different)

```
cd /path/to/DBAPI.jl

-- Start the julia repl

julia> Pkg.clone("https://github.com/JuliaDB/DBI.jl")

INFO: Cloning DBAPI from /path/to/DBAPI.jl
INFO: Computing changes...
INFO: No packages to install, update or remove
INFO: Package database updated

julia> Pkg.test("DBAPI")
INFO: Computing test dependencies for DBAPI...
INFO: Installing FactCheck v0.4.3
INFO: Testing DBAPI
Failed Interface
168 facts verified.
Array interface
268 facts verified.
INFO: DBAPI tests passed
INFO: Removing FactCheck v0.4.3
```