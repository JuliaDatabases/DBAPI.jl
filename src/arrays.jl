module ArrayInterfaces

import Base: connect, close, getindex, start, next, done, length, isopen, isempty
importall ..DBAPIBase
import Compat.view


### Underlying column data structures

abstract AbstractColumn{T}

immutable Column{T} <: AbstractColumn{T}
    name::Symbol
    data::Vector{T}
end

immutable SubColumn{T} <: AbstractColumn{T}
    name::Symbol
    data::SubArray{T}
end

SubColumn(column::Column, indices...) = SubColumn(column.name, view(column.data, indices...))

start(column::AbstractColumn) = start(column.data)
next{T}(column::AbstractColumn{T}, state) = next(column.data, state)::Tuple{T, Any}
done(column::AbstractColumn, state) = done(column.data, state)

getindex(column::AbstractColumn, indexes...) = getindex(column.data, indexes...)
length(column::AbstractColumn) = length(column.data)

function allequal{T}(things::AbstractArray{T})
    thing_set = Set{T}()

    for thing in things
        push!(thing_set, thing)

        if length(thing_set) > 1
            return false
        end
    end

    return true
end


### Interface

type ColumnarArrayInterface <: DatabaseInterface end


### Connections

type ColumnarArrayConnection <: DatabaseConnection{ColumnarArrayInterface}
    columns::Vector{Column}
    closed::Bool
end

function connect(
        ::Type{ColumnarArrayInterface},
        names::AbstractArray{Symbol},
        columns::AbstractArray{Vector}
    )

    if length(names) != length(columns)
        throw(ArrayInterfaceError("Arrays of names and columns must be the same length"))
    end

    if !allequal(map(length, columns))
        throw(ArrayInterfaceError("Columns must be the same length"))
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


### Cursors

type ColumnarArrayCursor <: FixedLengthDatabaseCursor{ColumnarArrayInterface}
    connection::ColumnarArrayConnection
    columns::Vector{SubColumn}

    ColumnarArrayCursor(connection) = new(connection)
end

cursor(connection::ColumnarArrayConnection) = ColumnarArrayCursor(connection)

connection(cursor::ColumnarArrayCursor) = cursor.connection

isempty(cursor::ColumnarArrayCursor) = isempty(cursor.columns)

length(cursor::ColumnarArrayCursor) = isempty(cursor) ? 0 : length(first(cursor.columns))


### Queries

immutable ColumnarArrayQuery{T<:OrdinalRange} <: DatabaseQuery
    columns::Vector{Symbol}
    rows::T
end

function execute!(cursor::ColumnarArrayCursor, query::ColumnarArrayQuery)
    try
        # handle case where there are rows requested from no columns
        if isempty(query.columns) && !isempty(query.rows)
            throw(BoundsError())
        end

        remaining_columns = Set(query.columns)
        cursor.columns = map(filter(cursor.connection.columns) do col
                is_queried = col.name in query.columns
                if is_queried
                    pop!(remaining_columns, col.name)
                end
                is_queried
            end) do col
            SubColumn(col, query.rows)
        end

        # don't allow queries for nonexistent columns
        if !isempty(remaining_columns)
            throw(BoundsError())
        end
    catch error
        if isa(error, BoundsError) || isa(error, KeyError)
            rethrow(DatabaseQueryError(interface(cursor), query))
        else
            rethrow(error)
        end
    end

    return nothing
end


### Results

immutable ColumnarArrayRowIterator
    cursor::ColumnarArrayCursor
end

function rows(cursor::ColumnarArrayCursor)
    if !isdefined(cursor, :columns)
        throw(ArrayInterfaceError("No query has been run on $cursor"))
    end

    return ColumnarArrayRowIterator(cursor)
end

function start(iter::ColumnarArrayRowIterator)
    if isempty(iter.cursor.columns)
        # needed because the method zip() does not exist
        zip_iterator = zip(())
    else
        zip_iterator = zip(iter.cursor.columns...)
    end

    return (zip_iterator, start(zip_iterator))
end

function next(iter::ColumnarArrayRowIterator, state)
    (zip_iterator, current_state) = state
    (next_set, next_state) = next(zip_iterator, current_state)
    return (next_set, (zip_iterator, next_state))
end

function done(iter::ColumnarArrayRowIterator, state)
    return done(state...)
end

immutable ColumnarArrayColumnIterator
    cursor::ColumnarArrayCursor
end

function columns(cursor::ColumnarArrayCursor)
    if !isdefined(cursor, :columns)
        throw(ArrayInterfaceError("No query has been run on $cursor"))
    end

    return ColumnarArrayColumnIterator(cursor)
end

start(iter::ColumnarArrayColumnIterator) = start(iter.cursor.columns)

function next(iter::ColumnarArrayColumnIterator, state)
    (next_col, next_state) = next(iter.cursor.columns, state)
    return (collect(next_col), next_state)
end

done(iter::ColumnarArrayColumnIterator, state) = done(iter.cursor.columns, state)

function getindex(cursor::ColumnarArrayCursor, row_ind::Int, column_name::Symbol)
    if_failed = BoundsError(cursor, (row_ind, column_name))

    for column in cursor.columns
        if column.name == column_name
            try
                return column[row_ind]
            catch error
                rethrow(if_failed)
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
        rethrow(if_failed)
    end
end


### Errors

immutable ArrayInterfaceError{T<:AbstractString} <: DatabaseError{ColumnarArrayInterface}
    message::T
end

if VERSION >= v"0.5-"
    Base.iteratorsize(::ColumnarArrayRowIterator) = Base.SizeUnknown()
    Base.iteratoreltype(::ColumnarArrayRowIterator) = Base.EltypeUnknown()
    Base.iteratorsize(::ColumnarArrayColumnIterator) = Base.SizeUnknown()
    Base.iteratoreltype(::ColumnarArrayColumnIterator) = Base.EltypeUnknown()
end

end  # module
