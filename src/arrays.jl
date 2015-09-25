module ArrayInterfaces

import Base: connect, close, getindex, show, start, next, done, length, isopen
importall ..DBAPIBase
import Iterators: partition

abstract AbstractColumn{T}

immutable Column{T} <: AbstractColumn{T}
    name::Symbol
    data::Vector{T}
end

immutable SubColumn{T} <: AbstractColumn{T}
    name::Symbol
    data::SubArray{T}
end

SubColumn(column::Column, indices...) = SubColumn(column.name, sub(column.data, indices...))

start(column::AbstractColumn) = start(column.data)
next{T}(column::AbstractColumn{T}, state) = next(column.data, state)::(T, Any)
done(column::AbstractColumn, state) = done(column.data, state)

getindex(column::AbstractColumn, indexes...) = getindex(column.data, indexes...)
length(column::AbstractColumn) = length(column.data)

type ColumnarArrayInterface <: DatabaseInterface end

immutable ArrayInterfaceError{T<:AbstractString} <: DatabaseError{ColumnarArrayInterface}
    message::T
end

type ColumnarArrayConnection <: DatabaseConnection{ColumnarArrayInterface}
    columns::Vector{Column}
    closed::Bool
end

function Base.show(io::IO, connection::ColumnarArrayConnection)
    print(io, typeof(connection), "(closed=$(!isopen(connection))")
end

type ColumnarArrayCursor <: DatabaseCursor{ColumnarArrayInterface}
    connection::ColumnarArrayConnection
    columns::Vector{SubColumn}

    ColumnarArrayCursor(connection) = new(connection)
end

function Base.show(io::IO, cursor::ColumnarArrayCursor)
    print(io, typeof(cursor), " for ", cursor.connection)
end


abstract ColumnarArrayRowIterator

type ColumnarArrayInfiniteRowIterator
    cursor::ColumnarArrayCursor
end

type ColumnarArrayStepRowIterator
    cursor::ColumnarArrayCursor
    nrows::Int
end

type ColumnarArrayColumnIterator
    cursor::ColumnarArrayCursor
end

function rows(cursor::ColumnarArrayCursor, nrows)
    if nrows == Inf
        return ColumnarArrayInfiniteRowIterator(cursor)
    else
        return ColumnarArrayStepRowIterator(cursor, convert(Int64, nrows))
    end
end

start(iter::ColumnarArrayInfiniteRowIterator) = true
function next(iter::ColumnarArrayInfiniteRowIterator, state::Bool)
    if state
        return collect(zip(iter.cursor.columns...)), false
    end
end
done(iter::ColumnarArrayInfiniteRowIterator, state::Bool) = !state

function start(iter::ColumnarArrayStepRowIterator)
    part_iterator = partition(zip(iter.cursor.columns...), iter.nrows)
    return (part_iterator, start(part_iterator))
end

function next(iter::ColumnarArrayStepRowIterator, state)
    (part_iterator, current_state) = state
    (next_set, next_state) = next(part_iterator, current_state)
    return (collect(next_set), (part_iterator, next_state))
end

function done(iter::ColumnarArrayStepRowIterator, state)
    return done(state...)
end

columns(cursor::ColumnarArrayCursor) = ColumnarArrayColumnIterator(cursor)

start(iter::ColumnarArrayColumnIterator) = start(iter.cursor.columns)
next(iter::ColumnarArrayColumnIterator, state) = next(iter.cursor.columns, state)
done(iter::ColumnarArrayColumnIterator, state) = done(iter.cursor.columns, state)

immutable ColumnarArrayQuery{T<:OrdinalRange} <: DatabaseQuery
    columns::Vector{Symbol}
    rows::T
end

function connect(
        ::Type{ColumnarArrayInterface},
        names::AbstractArray{Symbol},
        columns::AbstractArray{Vector}
    )
    if length(names) != length(columns)
        throw(ArrayInterfaceError("Arrays of names and columns must be the same length"))
    end

    ColumnarArrayConnection(
        map(names, columns) do name, column
            Column(name, column)
        end,
        false,
    )
end

function close(connection::ColumnarArrayConnection)
    connection.closed = true
    empty!(connection.columns)

    return nothing
end

# does not support transactions
commit(connection::ColumnarArrayConnection) = nothing

isopen(connection::ColumnarArrayConnection) = !connection.closed

cursor(connection::ColumnarArrayConnection) = ColumnarArrayCursor(connection)

function execute!(cursor::ColumnarArrayCursor, query::ColumnarArrayQuery)
    try
        cursor.columns = map(filter(cursor.connection.columns) do col
                col.name in query.columns
            end) do col
            SubColumn(col, query.rows)
        end
    catch error
        if isa(error, BoundsError)
            throw(DatabaseQueryError(query))
        else
            rethrow(error)
        end
    end
end

function getindex(cursor::ColumnarArrayCursor, row_ind::Int, column_name::Symbol)
    if_failed = BoundsError(cursor, (row_ind, column_name))

    for column in cursor.columns
        if column.name == column_name
            try
                return column[row_ind]
            catch error
                throw(if_failed)
            end
        end
    end

    throw(if_failed)
end

function getindex(cursor::ColumnarArrayCursor, row_ind::Int, column_ind::Int)
    if_failed = BoundsError(cursor, (row_ind, column_ind))

    try
        return cursor.columns[column_ind][row_ind]
    catch error
        throw(if_failed)
    end
end

end  # module
