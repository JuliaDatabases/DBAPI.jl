module DBAPI

import Base: connect, close, getindex

abstract DatabaseInterface
abstract DatabaseError{T<:DatabaseInterface} <: Exception
abstract DatabaseConnection{T<:DatabaseInterface}
abstract DatabaseCursor{T<:DatabaseInterface} <: AbstractArray
Base.linearindexing(::Type{DatabaseCursor}) = Base.LinearSlow()
Base.ndims(cursor::DatabaseCursor) = 2

"""
If this error is thrown, a driver has not implemented a required function
of this interface.
"""
type NotImplementedError{T<:DatabaseInterface} <: DatabaseError{T} end
function Base.showerror{T<:DatabaseInterface}(io::IO, e::NotImplementedError{T})
    print(io, T, " does not implement this required DBAPI feature")
end

"""
If this error is thrown, a user has attempted to use an optional function
of this interface which the driver does not implement.
"""
type NotSupportedError{T<:DatabaseInterface} <: DatabaseError{T} end
function Base.showerror{T<:DatabaseInterface}(io::IO, e::NotSupportedError{T})
    print(io, T, " does not support this optional DBAPI feature")
end


"""
Constructs a database connection.

Returns `connection::DatabaseConnection`.
"""
function connect{T<:DatabaseInterface}(::Type{T}, args...; kwargs...)
    throw(NotImplementedError{T}())
end

"""
Close the connection now (rather than when the finalizer is called).

Any further attempted operations on the connection or its cursors will throw a
subtype of DatabaseError.

Closing a connection without committing will cause an implicit rollback to be
performed.

Returns `nothing`.
"""
function close{T<:DatabaseInterface}(conn::DatabaseConnection{T})
    throw(NotImplementedError{T}())
end

"""
Commit any pending transaction to the database.

Dataase drivers that do not support transactions should implement this
function with no body.

Returns `nothing`.
"""
function commit{T<:DatabaseInterface}(conn::DatabaseConnection{T})
    throw(NotImplementedError{T}())
end

"""
Roll back to the start of any pending transaction.

Database drivers that do not support transactions may not implement this
function.

Returns `nothing`.
"""
function rollback{T<:DatabaseInterface}(conn::DatabaseConnection{T})
    throw(NotSupportedError{T}())
end

"""
Create a new database cursor.

If the database does not implement cursors, the driver must implement a cursor
object which emulates cursors to the extent required by the interface.

Some drivers may implement multiple cursor types, but all must follow the
`DatabaseCursor` interface. Additional arguments may be given to the
driver's implementation of `cursor` but this method must be implemented with
reasonable defaults.

Returns `cursor::DatabaseCursor`.
"""
function cursor{T<:DatabaseInterface}(conn::DatabaseConnection{T})
    throw(NotImplementedError{T}())
end

"""
Run a query on a database.

The results of the query are not returned by this function but are accessible
through the cursor.

`parameters` can be any iterable of positional parameters, or of some
T<:Associative for keyword/named parameters.

Returns `nothing`.
"""
function execute{T<:DatabaseInterface}(
        cursor::DatabaseCursor{T},
        query::AbstractString,
        parameters=(),
    )
    throw(NotImplementedError{T}())
end

"""
Run a query on a database multiple times with different parameters.

The results of the queries are not returned by this function. The result of
the final query run is accessible by the cursor.

`parameters` can be any iterable of a set of any iterables of positional
parameters, or items of some T<:Associative for keyword/named parameters.

Returns `nothing`.
"""
function executemany{T<:DatabaseInterface}(
        cursor::DatabaseCursor{T},
        query::AbstractString,
        parameters=(),
    )
    for parameter_set in parameters
        result = execute(cursor, query, parameter_set)
    end

    return nothing
end

"""
Create a row iterator.

This method should return an instance of an iterator type which returns at
most `nrows` rows on each iteration. Use `Inf` for an iterator which will
return all the rows in one iteration. Row groups should be returned as a
Vector{Tuple{...}} with as much type information in the Tuple{...} as
possible. It is encouraged but not necessary to have the row groups be of the
same type.
"""
function rows{T<:DatabaseInterface}(cursor::DatabaseCursor{T}, nrows::Real=Inf)
    throw(NotImplementedError{T}())
end

"""
Create a column iterator.

This method should return an instance of an iterator type which returns one
column on each iteration. Columns should be returned as a Vector{...} with as
much type information in the Vector{...} as possible.

This method is optional if rows can have different lengths or sets of values.
"""
function columns{T<:DatabaseInterface}(cursor::DatabaseCursor{T})
    throw(NotSupportedError{T}())
end

"""
Get result value from a database cursor.

This method gets a single result value in row `i` in column `j`.

This method is optional if rows or columns do not have a defined order.
"""
function getindex{T<:DatabaseInterface}(cursor::DatabaseCursor{T}, i::Integer, j::Integer)
    throw(NotSupportedError{T}())
end

"""
Get result value from a database cursor.

This method gets a single result value in row `i` in column named `col`.

This method is optional if rows do not have a defined order or if columns do
not have names.
"""
function getindex{T<:DatabaseInterface}(cursor::DatabaseCursor{T}, i::Integer, col::Symbol)
    throw(NotSupportedError{T}())
end

"""
Get result value from a database cursor.

This method gets a single result value in row named `row` in column `j`.

This method is optional if rows do not have names/keys or if columns do not
have a defined order.
"""
function getindex{T<:DatabaseInterface}(cursor::DatabaseCursor{T}, row::Symbol, j::Integer)
    throw(NotSupportedError{T}())
end

"""
Get result value from a database cursor.

This method gets a single result value in row named `row` in column named `col`.

This method is optional if rows do not have names/keys or if columns do not
have names.
"""
function getindex{T<:DatabaseInterface}(cursor::DatabaseCursor{T}, row::Symbol, col::Symbol)
    throw(NotSupportedError{T}())
end

"""
Get result value from a database cursor.

This method gets a single result value in row indexed by `row` in column
indexed by `col`.

Any other row or column index types are optional.
"""
function getindex{T<:DatabaseInterface}(cursor::DatabaseCursor{T}, row::Any, col::Any)
    throw(NotSupportedError{T}())
end

"""
A terrible hack to make the fetchinto! signature work.

See https://github.com/JuliaLang/julia/issues/13156#issuecomment-140618981
"""
typealias AssociativeVK{V,K} Associative{K,V}

index_return_type(a::Associative) = valtype(a)
index_return_type(a::Any) = eltype(a)

"""
Get results from a database cursor and store them in a preallocated data
structure.

This out-of-the-box method supports a huge variety of data structures under the
`AbstractArray` and `Associative` supertypes. It uses the `getindex` functions
defined above.

Returns the preallocated data structure.
"""
function fetchinto!{T<:DatabaseInterface, U<:Union{AbstractArray, Associative}}(
        preallocated::Union{AbstractArray{U}, AssociativeVK{U}},
        cursor::DatabaseCursor{T}
    )
    for i in eachindex(preallocated), j in eachindex(preallocated[i])
        datum = cursor[i, j]
        preallocated[i][j] = (
            isa(datum, Nullable) && !(index_return_type(preallocated[i]) <: Nullable) ?
            get(datum) :
            datum
        )
    end

    return preallocated
end

end # module
