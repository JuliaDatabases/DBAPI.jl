module FailedInterface

import DBAPI
using FactCheck
using DataStructures

type BadInterface <: DBAPI.DatabaseInterface end
type BadConnection <: DBAPI.DatabaseConnection{BadInterface} end
type BadCursor <: DBAPI.DatabaseCursor{BadInterface} end
type BadFixedLengthCursor <: DBAPI.FixedLengthDatabaseCursor{BadInterface} end
type BadQuery <: DBAPI.DatabaseQuery end

facts("Failed Interface") do
    # data structures for testing
    empty_data_structures = (
        Array{Any}[Array{Any}(0)],
        Array{Any}[Array{Any}(0, 0)],
        Dict{Any, Any}[Dict{Any, Any}()],
        PriorityQueue[PriorityQueue()],
        Dict{Any,Array{Any}}(1=>Array{Any}(0)),
        Dict{Any,Array{Any}}(1=>Array{Any}(0, 0)),
        Array{Nullable{Any}}[Array{Nullable{Any}}(0)],
        Array{Nullable{Any}}[Array{Nullable{Any}}(0, 0)],
        Dict{Any, Nullable{Any}}[Dict{Any, Nullable{Any}}()],
        Dict{Any,Array{Nullable{Any}}}(1=>Array{Nullable{Any}}(0)),
        Dict{Any,Array{Nullable{Any}}}(1=>Array{Nullable{Any}}(0, 0)),
    )

    empty_2d_data_structures = (
        Array{Any}(0,0),
        Dict{Any, Any}(),
        PriorityQueue(),
        Array{Nullable{Any}}(0,0),
        Dict{Any, Nullable{Any}}(),
    )

    filled_pq = PriorityQueue()
    filled_pq[1] = 1

    filled_data_structures = (
        Array{Any}[Array{Any}(1)],
        Array{Any}[Array{Any}(1, 1)],
        Dict{Any, Any}[Dict{Any, Any}(1=>5)],
        PriorityQueue[filled_pq],
        Dict{Any,Array{Nullable{Any}}}(1=>Array{Nullable{Any}}(1)),
        Array{Nullable{Any}}[Array{Nullable{Any}}(1)],
        Array{Nullable{Any}}[Array{Nullable{Any}}(1, 1)],
        Dict{Any, Nullable{Any}}[Dict{Any, Nullable{Any}}(1=>5)],
        Dict{Any,Array{Nullable{Any}}}(1=>Array{Nullable{Any}}(1)),
    )

    filled_2d_pq = PriorityQueue()
    filled_2d_pq[1, 1] = 1

    filled_2d_data_structures = (
        Array{Any}(1),
        Array{Any}(1, 1),
        Dict{Any, Any}((1, 1)=>5),
        filled_2d_pq,
        Array{Nullable{Any}}(1),
        Array{Nullable{Any}}(1, 1),
        Dict{Any, Nullable{Any}}((1, 1)=>Nullable{Any}(5)),
    )

    dummy_io = IOBuffer()

    @fact_throws DBAPI.NotImplementedError DBAPI.connect(BadInterface)
    @fact_throws DBAPI.NotImplementedError DBAPI.connect(BadInterface, "foobar")
    @fact_throws DBAPI.NotImplementedError DBAPI.connect(BadInterface, "foobar"; port=2345)
    @fact_throws DBAPI.NotImplementedError DBAPI.connect(identity, BadInterface)

    connection = BadConnection()

    @fact_throws DBAPI.NotImplementedError Base.show(connection)
    @fact_throws DBAPI.NotImplementedError DBAPI.commit(connection)
    @fact_throws DBAPI.NotImplementedError DBAPI.isopen(connection)
    @fact_throws DBAPI.NotImplementedError DBAPI.close(connection)
    @fact_throws DBAPI.NotImplementedError DBAPI.isopen(connection)
    @fact_throws DBAPI.NotImplementedError DBAPI.commit(connection)
    @fact_throws DBAPI.NotSupportedError DBAPI.rollback(connection)
    @fact_throws DBAPI.NotImplementedError DBAPI.cursor(connection)

    # no length
    cursor = BadCursor()
    @fact isa(cursor, DBAPI.DatabaseCursor) --> true
    @fact_throws DBAPI.NotSupportedError length(cursor)

    for i in [12, :twelve]
        for j in [12, :twelve]
            @fact_throws DBAPI.NotSupportedError cursor[i, j]
        end
    end

    @fact_throws DBAPI.NotSupportedError cursor["far", Set([1,2,3])]

    for ds in empty_2d_data_structures
        @fact_throws DBAPI.NotSupportedError DBAPI.fetchintoarray!(ds, cursor)
    end

    for ds in empty_data_structures
        @fact_throws DBAPI.NotSupportedError DBAPI.fetchintoarray!(ds, cursor)
    end

    for ds in empty_data_structures
        @fact_throws DBAPI.NotSupportedError DBAPI.fetchintoarray!(ds, cursor)
    end

    for ds in filled_2d_data_structures
        @fact_throws DBAPI.NotSupportedError DBAPI.fetchintoarray!(ds, cursor)
    end

    for ds in filled_data_structures
        @fact_throws DBAPI.NotSupportedError DBAPI.fetchintorows!(ds, cursor)
    end

    for ds in filled_data_structures
        @fact_throws DBAPI.NotSupportedError DBAPI.fetchintocolumns!(ds, cursor)
    end

    # fixed length
    cursor = BadFixedLengthCursor()
    @fact isa(cursor, DBAPI.DatabaseCursor) --> true
    @fact isa(cursor, DBAPI.FixedLengthDatabaseCursor) --> true

    @fact_throws DBAPI.NotImplementedError DBAPI.connection(cursor)
    @fact_throws DBAPI.NotImplementedError Base.show(cursor)

    @fact_throws DBAPI.NotImplementedError DBAPI.rows(cursor)
    @fact_throws DBAPI.NotSupportedError DBAPI.columns(cursor)

    @fact_throws DBAPI.NotSupportedError DBAPI.execute!(cursor, "foobar", (1, "d"))

    for i in [12, :twelve]
        for j in [12, :twelve]
            @fact_throws DBAPI.NotImplementedError cursor[i, j]
        end
    end

    @fact_throws DBAPI.NotImplementedError cursor["far", Set([1,2,3])]

    for ds in empty_2d_data_structures
        @fact DBAPI.fetchintoarray!(ds, cursor) --> exactly((ds, 0))
    end

    for ds in empty_data_structures
        # in this case there is a row, it's just empty
        @fact DBAPI.fetchintorows!(ds, cursor) --> exactly((ds, 1))
    end

    for ds in empty_data_structures
        @fact DBAPI.fetchintocolumns!(ds, cursor) --> exactly((ds, 0))
    end

    for ds in filled_2d_data_structures
        @fact_throws DBAPI.NotImplementedError DBAPI.fetchintoarray!(ds, cursor)
    end

    for ds in filled_data_structures
        @fact_throws DBAPI.NotImplementedError DBAPI.fetchintorows!(ds, cursor)
    end

    for ds in filled_data_structures
        @fact_throws DBAPI.NotImplementedError DBAPI.fetchintocolumns!(ds, cursor)
    end

    # testing that these methods exist and run
    # @fact_throws does not do that, unfortunately
    Base.showerror(dummy_io, DBAPI.NotImplementedError{BadInterface}())
    Base.showerror(dummy_io, DBAPI.NotSupportedError{BadInterface}())
    Base.showerror(dummy_io, DBAPI.DatabaseQueryError(BadInterface, BadQuery()))

    for empty_ds in empty_data_structures
        @fact_throws DBAPI.NotImplementedError begin
            for ds in DBAPI.DatabaseFetcher(:rows, empty_ds, cursor)
                @fact true --> false  # should never be reached
            end
        end
    end

    for empty_ds in empty_data_structures
        @fact_throws DBAPI.NotImplementedError begin
            for ds in DBAPI.DatabaseFetcher(:columns, empty_ds, cursor)
                @fact true --> false  # should never be reached
            end
        end
    end

    for empty_ds in empty_data_structures
        @fact_throws DBAPI.NotImplementedError begin
            for ds in DBAPI.DatabaseFetcher(:array, empty_ds, cursor)
                @fact true --> false  # should never be reached
            end
        end
    end
end

end
