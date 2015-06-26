# DBAPI

This module defines an abstract interface for database drivers in Julia.

This module contains abstract types, abstract required functions (which throw
a `NotImplementedError` by default), and abstract optional functions (which
throw a `NotSupportedError` by default).

Database drivers should import this module, subtype its types, and create
methods for its functions which implement the signatures and behavioural
specifications contained within this module.

This interface is largely inspired by Python's DB API 2.0, which is in the
public domain: [PEP0249](https://www.python.org/dev/peps/pep-0249).
