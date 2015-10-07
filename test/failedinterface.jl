module FailedInterface

import DBAPI
using Base.Test
import Base.Collections: PriorityQueue

type BadInterface <: DBAPI.DatabaseInterface end
type BadConnection <: DBAPI.DatabaseConnection{BadInterface} end
type BadCursor <: DBAPI.DatabaseCursor{BadInterface} end
type BadFixedLengthCursor <: DBAPI.FixedLengthDatabaseCursor{BadInterface} end
type BadQuery <: DBAPI.DatabaseQuery end

function main()
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

    @test_throws DBAPI.NotImplementedError DBAPI.connect(BadInterface)
    @test_throws DBAPI.NotImplementedError DBAPI.connect(BadInterface, "foobar")
    @test_throws DBAPI.NotImplementedError DBAPI.connect(BadInterface, "foobar"; port=2345)

    connection = BadConnection()

    @test_throws DBAPI.NotImplementedError Base.show(connection)
    @test_throws DBAPI.NotImplementedError DBAPI.commit(connection)
    @test_throws DBAPI.NotImplementedError DBAPI.isopen(connection)
    @test_throws DBAPI.NotImplementedError DBAPI.close(connection)
    @test_throws DBAPI.NotImplementedError DBAPI.isopen(connection)
    @test_throws DBAPI.NotImplementedError DBAPI.commit(connection)
    @test_throws DBAPI.NotSupportedError DBAPI.rollback(connection)
    @test_throws DBAPI.NotImplementedError DBAPI.cursor(connection)

    # no length
    cursor = BadCursor()
    @test isa(cursor, DBAPI.DatabaseCursor)
    @test_throws DBAPI.NotSupportedError length(cursor)

    for i in [12, :twelve]
        for j in [12, :twelve]
            @test_throws DBAPI.NotSupportedError cursor[i, j]
        end
    end

    @test_throws DBAPI.NotSupportedError cursor["far", Set([1,2,3])]

    for ds in empty_2d_data_structures
        @test_throws DBAPI.NotSupportedError DBAPI.fetchintoarray!(ds, cursor)
    end

    for ds in empty_data_structures
        @test_throws DBAPI.NotSupportedError DBAPI.fetchintoarray!(ds, cursor)
    end

    for ds in empty_data_structures
        @test_throws DBAPI.NotSupportedError DBAPI.fetchintoarray!(ds, cursor)
    end

    for ds in filled_2d_data_structures
        @test_throws DBAPI.NotSupportedError DBAPI.fetchintoarray!(ds, cursor)
    end

    for ds in filled_data_structures
        @test_throws DBAPI.NotSupportedError DBAPI.fetchintorows!(ds, cursor)
    end

    for ds in filled_data_structures
        @test_throws DBAPI.NotSupportedError DBAPI.fetchintocolumns!(ds, cursor)
    end

    # fixed length
    cursor = BadFixedLengthCursor()
    @test isa(cursor, DBAPI.DatabaseCursor)
    @test isa(cursor, DBAPI.FixedLengthDatabaseCursor)

    @test_throws DBAPI.NotImplementedError DBAPI.connection(cursor)
    @test_throws DBAPI.NotImplementedError Base.show(cursor)

    @test_throws DBAPI.NotImplementedError DBAPI.rows(cursor)
    @test_throws DBAPI.NotSupportedError DBAPI.columns(cursor)

    @test_throws DBAPI.NotImplementedError DBAPI.execute!(cursor, "foobar", (1, "d"))
    @test_throws DBAPI.NotImplementedError DBAPI.executemany!(cursor, "foobar", ((1, "d"), ("6", 0xd)))

    for i in [12, :twelve]
        for j in [12, :twelve]
            @test_throws DBAPI.NotImplementedError cursor[i, j]
        end
    end

    @test_throws DBAPI.NotImplementedError cursor["far", Set([1,2,3])]

    for ds in empty_2d_data_structures
        @test (ds, 0) === DBAPI.fetchintoarray!(ds, cursor)
    end

    for ds in empty_data_structures
        # in this case there is a row, it's just empty
        @test (ds, 1) === DBAPI.fetchintorows!(ds, cursor)
    end

    for ds in empty_data_structures
        @test (ds, 0) === DBAPI.fetchintocolumns!(ds, cursor)
    end

    for ds in filled_2d_data_structures
        @test_throws DBAPI.NotImplementedError DBAPI.fetchintoarray!(ds, cursor)
    end

    for ds in filled_data_structures
        @test_throws DBAPI.NotImplementedError DBAPI.fetchintorows!(ds, cursor)
    end

    for ds in filled_data_structures
        @test_throws DBAPI.NotImplementedError DBAPI.fetchintocolumns!(ds, cursor)
    end

    # testing that these methods exist and run
    # @test_throws does not do that, unfortunately
    Base.showerror(dummy_io, DBAPI.NotImplementedError{BadInterface}())
    Base.showerror(dummy_io, DBAPI.NotSupportedError{BadInterface}())
    Base.showerror(dummy_io, DBAPI.DatabaseQueryError(BadInterface, BadQuery()))

    for empty_ds in empty_data_structures
        @test_throws DBAPI.NotImplementedError begin
            for ds in DBAPI.DatabaseFetcher(:rows, empty_ds, cursor)
                @test false  # should never be reached
            end
        end
    end

    for empty_ds in empty_data_structures
        @test_throws DBAPI.NotImplementedError begin
            for ds in DBAPI.DatabaseFetcher(:columns, empty_ds, cursor)
                @test false  # should never be reached
            end
        end
    end

    for empty_ds in empty_data_structures
        @test_throws DBAPI.NotImplementedError begin
            for ds in DBAPI.DatabaseFetcher(:array, empty_ds, cursor)
                @test false  # should never be reached
            end
        end
    end
end

end

FailedInterface.main()
