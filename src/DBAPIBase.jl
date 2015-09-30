module DBAPIBase

export cursor,
    execute!,
    executemany!,
    commit,
    rollback,
    rows,
    columns,
    fetchinto!,
    fetchrowsinto!,
    fetchcolumnsinto!,
    interface,
    DatabaseInterface,
    DatabaseError,
    DatabaseConnection,
    DatabaseCursor,
    DatabaseQuery,
    DatabaseQueryError


import Base: connect, close, getindex, isopen

abstract DatabaseInterface
abstract DatabaseError{T<:DatabaseInterface} <: Exception
abstract DatabaseConnection{T<:DatabaseInterface}
abstract DatabaseCursor{T<:DatabaseInterface}
Base.linearindexing(::Type{DatabaseCursor}) = Base.LinearSlow()
Base.ndims(cursor::DatabaseCursor) = 2

abstract DatabaseQuery

immutable StringDatabaseQuery{T<:AbstractString} <: DatabaseQuery
    query::T
end

"""
Returns the interface type for any database object.
"""
function interface{T<:DatabaseInterface}(
    database_object::Union{DatabaseCursor{T}, DatabaseConnection{T}, DatabaseError{T}}
)
    return T
end

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
If this error is thrown, an error occured while processing this database query.
"""
type DatabaseQueryError{T<:DatabaseInterface, S<:DatabaseQuery} <: DatabaseError{T}
    interface::Type{T}
    query::S
end

function Base.showerror{T<:DatabaseInterface}(io::IO, e::DatabaseQueryError{T})
    print(io, "An error occured while processing this query:\n", e.query)
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
Returns true if the connection is open and not broken.

Returns `Bool`
"""
function isopen{T<:DatabaseInterface}(conn::DatabaseConnection{T})
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
function execute!{T<:DatabaseInterface}(
        cursor::DatabaseCursor{T},
        query::DatabaseQuery,
        parameters=(),
    )
    throw(NotImplementedError{T}())
end

function execute!{T<:DatabaseInterface}(
        cursor::DatabaseCursor{T},
        query::AbstractString,
        parameters=(),
    )
    execute!(cursor, StringDatabaseQuery(query), parameters)
end

"""
Run a query on a database multiple times with different parameters.

The results of the queries are not returned by this function. The result of
the final query run is accessible by the cursor.

`parameters` can be any iterable of a set of any iterables of positional
parameters, or items of some T<:Associative for keyword/named parameters.

Returns `nothing`.
"""
function executemany!{T<:DatabaseInterface}(
        cursor::DatabaseCursor{T},
        query::DatabaseQuery,
        parameters=(),
    )
    for parameter_set in parameters
        result = execute!(cursor, query, parameter_set)
    end

    return nothing
end

function executemany!{T<:DatabaseInterface}(
        cursor::DatabaseCursor{T},
        query::AbstractString,
        parameters=(),
    )
    executemany!(cursor, StringDatabaseQuery(query), parameters)
end

"""
Create a row iterator.

This method should return an instance of an iterator type which returns one row
on each iteration. Each row should be returned as a Tuple{...} with as much
type information in the Tuple{...} as possible. It is encouraged but not
necessary to have the rows be of the same type.
"""
function rows{T<:DatabaseInterface}(cursor::DatabaseCursor{T})
    throw(NotImplementedError{T}())
end

"""
Create a column iterator.

This method should return an instance of an iterator type which returns one
column on each iteration. Each column should be returned as a Vector{...} with
as much type information in the Vector{...} as possible.

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
typealias AssociativeVK{V, K} Associative{K, V}

index_return_type(a::Associative) = valtype(a)
index_return_type(a::Any) = eltype(a)

each_index_tuple(a::Associative) = eachindex(a)
each_index_tuple(a::Any) = map(ind -> ind2sub(a, ind), eachindex(a))

"""
Get results from a database cursor and store them in a preallocated
two-dimensional data structure.

This out-of-the-box method supports a huge variety of data structures under the
`AbstractArray` and `Associative` supertypes. It uses the `getindex` functions
defined above.

Returns the preallocated data structure.
"""
function fetchinto!{T<:DatabaseInterface}(
        preallocated::Union{AbstractMatrix, Associative},
        cursor::DatabaseCursor{T}
    )
    for (i, j) in each_index_tuple(preallocated)
        datum = cursor[i, j]
        preallocated[i, j] = (
            isa(datum, Nullable) &&
            !(index_return_type(preallocated) <: Nullable ||
                Nullable <: index_return_type(preallocated)) ?
            get(datum) :
            datum
        )
    end

    return preallocated
end

"""
Get results from a database cursor and store them in a preallocated data
structure (a collection of rows).

This out-of-the-box method supports a huge variety of data structures under the
`AbstractArray` and `Associative` supertypes. It uses the `getindex` functions
defined above.

Returns the preallocated data structure.
"""
function fetchrowsinto!{T<:DatabaseInterface, U<:Union{AbstractArray, Associative}}(
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

"""
Get results from a database cursor and store them in a preallocated data
structure (a collection of columns).

This out-of-the-box method supports a huge variety of data structures under the
`AbstractArray` and `Associative` supertypes. It uses the `getindex` functions
defined above.

Returns the preallocated data structure.
"""
function fetchcolumnsinto!{T<:DatabaseInterface, U<:Union{AbstractArray, Associative}}(
        preallocated::Union{AbstractArray{U}, AssociativeVK{U}},
        cursor::DatabaseCursor{T}
    )
    for j in eachindex(preallocated), i in eachindex(preallocated[j])
        datum = cursor[i, j]
        preallocated[j][i] = (
            isa(datum, Nullable) && !(index_return_type(preallocated[i]) <: Nullable) ?
            get(datum) :
            datum
        )
    end

    return preallocated
end

end # module
