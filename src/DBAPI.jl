"""
This module defines an abstract interface for database drivers in Julia.

This module contains abstract types, abstract required functions (which throw
a NotImplementedError by default), and abstract optional functions (which
throw a NotSupportedError by default).

Database drivers should import this module, subtype its types, and create
methods for its functions which implement the signatures and behavioural
specifications contained within this module.

This interface is largely inspired by Python's DB API 2.0, which is in the
public domain: [PEP0249](https://www.python.org/dev/peps/pep-0249).
"""
module DBAPI

import Base: connect, close

if VERSION < v"0.4.0-dev"
    using Docile
end

abstract AbstractDatabaseInterface
abstract AbstractDatabaseConnection{T<:AbstractDatabaseInterface}
abstract AbstractDatabaseCursor{T<:AbstractDatabaseInterface}
abstract AbstractDatabaseError{T<:AbstractDatabaseInterface} <: Exception

@doc """
If this error is thrown, a driver has not implemented a required function
of this interface.
""" ->
type NotImplementedError{T<:AbstractDatabaseInterface} <: AbstractDatabaseError{T} end
function Base.showerror{T<:AbstractDatabaseInterface}(io::IO, e::NotImplementedError{T})
    print(io, T, " does not implement this required DBAPI feature")
end

@doc """
If this error is thrown, a user has attempted to use an optional function
of this interface which the driver does not implement.
""" ->
type NotSupportedError{T<:AbstractDatabaseInterface} <: AbstractDatabaseError{T} end
function Base.showerror{T<:AbstractDatabaseInterface}(io::IO, e::NotSupportedError{T})
    print(io, T, " does not support this optional DBAPI feature")
end


@doc """
Constructs a database connection.

Returns `connection::AbstractDatabaseConnection`.
""" ->
function connect{T<:AbstractDatabaseInterface}(::Type{T}, args...; kwargs...)
    throw(NotImplementedError{T}())
end  # returns T<:AbstractDatabaseConnection

@doc """
Close the connection now (rather than when the finalizer is called).

Any further attempted operations on the connection or its cursors will throw a
subtype of AbstractDatabaseError.

Closing a connection without committing will cause an implicit rollback to be
performed.

Returns `nothing`.
""" ->
function close{T<:AbstractDatabaseInterface}(conn::AbstractDatabaseConnection{T})
    throw(NotImplementedError{T}())
end

@doc """
Commit any pending transaction to the database.

Dataase drivers that do not support transactions should implement this
function with no body.

Returns `nothing`.
""" ->
function commit{T<:AbstractDatabaseInterface}(conn::AbstractDatabaseConnection{T})
    throw(NotImplementedError{T}())
end

@doc """
Roll back to the start of any pending transaction.

Database drivers that do not support transactions may not implement this
function.

Returns `nothing`.
""" ->
function rollback{T<:AbstractDatabaseInterface}(conn::AbstractDatabaseConnection{T})
    throw(NotSupportedError{T}())
end

@doc """
Create a new database cursor.

If the database does not implement cursors, the driver must implement a cursor
object which emulates cursors to the extent required by the interface.

Returns `cursor::AbstractDatabaseCursor`
""" ->
function cursor{T<:AbstractDatabaseInterface}(conn::AbstractDatabaseConnection{T})
    throw(NotImplementedError{T}())
end
end # module
