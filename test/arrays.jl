module TestColumnarArrayInterface

using Base.Test
import Base.Collections: PriorityQueue

import Iterators: chain

import DBAPI
import DBAPI.ArrayInterfaces:
    ColumnarArrayInterface,
    ColumnarArrayQuery,
    ArrayInterfaceError


function main()
    # invalid
    @test_throws ArrayInterfaceError Base.connect(ColumnarArrayInterface, [:foo], Vector[])
    @test_throws ArrayInterfaceError Base.connect(ColumnarArrayInterface, Symbol[], Vector[[1, 2, 3]])
    @test_throws ArrayInterfaceError Base.connect(ColumnarArrayInterface, [:foo], Vector[[1], [2, 3]])

    # empty
    connection = Base.connect(ColumnarArrayInterface, Symbol[], Vector[])
    @test isa(connection, DBAPI.DatabaseConnection)
    @test DBAPI.isopen(connection)
    @test DBAPI.commit(connection) === nothing
    @test_throws DBAPI.NotSupportedError DBAPI.rollback(connection)
    @test Base.isopen(connection)

    cursor = DBAPI.cursor(connection)
    @test isa(cursor, DBAPI.DatabaseCursor)

    @test_throws ArrayInterfaceError DBAPI.rows(cursor)
    @test_throws ArrayInterfaceError DBAPI.columns(cursor)

    @test_throws DBAPI.DatabaseQueryError DBAPI.execute!(cursor, ColumnarArrayQuery([:one], 1:0))
    @test_throws DBAPI.DatabaseQueryError DBAPI.execute!(cursor, ColumnarArrayQuery(Symbol[], 1:1))
    @test_throws DBAPI.DatabaseQueryError DBAPI.execute!(cursor, ColumnarArrayQuery([:one], 1:1))

    @test DBAPI.execute!(cursor, ColumnarArrayQuery(Symbol[], 1:0)) === nothing

    @test isempty(collect(DBAPI.rows(cursor)))
    @test isempty(collect(DBAPI.columns(cursor)))
    @test_throws BoundsError cursor[1, 1]
    @test_throws BoundsError cursor[1, :one]
    @test isempty(cursor)
    @test length(cursor) == 0

    @test_throws DBAPI.NotImplementedError cursor[:one, 1]
    @test_throws DBAPI.NotImplementedError cursor[:one, :one]
    @test_throws DBAPI.NotImplementedError cursor["one", "one"]

    @test Base.close(connection) == nothing
    @test Base.isopen(connection) == false

    # non-empty
    connection = Base.connect(
        ColumnarArrayInterface,
        [:foo, :bar],
        Vector[[1, 2, 3], [3.0, 2.0, 1.0]]
    )
    @test isa(connection, DBAPI.DatabaseConnection)
    @test DBAPI.isopen(connection)
    @test DBAPI.commit(connection) === nothing
    @test_throws DBAPI.NotSupportedError DBAPI.rollback(connection)
    @test Base.isopen(connection)

    cursor = DBAPI.cursor(connection)
    @test isa(cursor, DBAPI.DatabaseCursor)

    @test_throws DBAPI.DatabaseQueryError DBAPI.execute!(cursor, ColumnarArrayQuery([:one], 1:1))

    try
        DBAPI.execute!(cursor, ColumnarArrayQuery(Symbol[], 1:0))
    catch error
        println(error)
        rethrow(error)
    end

    @test isempty(collect(DBAPI.rows(cursor)))
    @test isempty(collect(DBAPI.columns(cursor)))
    @test isempty(cursor)
    @test length(cursor) == 0
    @test_throws BoundsError cursor[1, 1]
    @test_throws BoundsError cursor[1, :one]

    @test DBAPI.execute!(cursor, ColumnarArrayQuery([:foo, :bar], 1:3)) === nothing
    row_results = [
        (1, 3.0),
        (2, 2.0),
        (3, 1.0),
    ]
    nullable_row_results = map(x -> map(Nullable{Any}, x), row_results)
    @test collect(DBAPI.rows(cursor)) == row_results
    @test collect(DBAPI.columns(cursor)) == Vector[
        [1, 2, 3],
        [3.0, 2.0, 1.0],
    ]

    @test cursor[3, :bar] == 1.0
    @test cursor[1, :foo] == 1
    @test_throws BoundsError cursor[4, :foo]
    @test_throws BoundsError cursor[1, :one]
    @test_throws BoundsError cursor[0, :one]

    @test !isempty(cursor)
    @test length(cursor) == length(row_results)

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

    for ds in empty_data_structures
        # ds will be a collection of an empty row
        @test (ds, 1) === DBAPI.fetchintorows!(ds, cursor)
    end

    for ds in empty_data_structures
        @test (ds, 0) === DBAPI.fetchintocolumns!(ds, cursor)
    end

    for ds in empty_2d_data_structures
        @test (ds, 0) === DBAPI.fetchintoarray!(ds, cursor)
    end

    for ds in filled_data_structures
        @test (ds, 1) === DBAPI.fetchintorows!(ds, cursor)
    end

    for ds in filled_data_structures
        @test (ds, 1) === DBAPI.fetchintocolumns!(ds, cursor)
    end

    for ds in filled_2d_data_structures
        @test (ds, 1) === DBAPI.fetchintoarray!(ds, cursor)
    end

    first_empty(a::Associative) = isempty(first(values(a)))
    first_empty(a) = isempty(first(a))

    for empty_ds in empty_data_structures
        iters = 0
        for ds in DBAPI.DatabaseFetcher(:rows, empty_ds, cursor)
            # ds will be a collection containing an empty row
            @test first_empty(ds)
            iters += 1
        end
        @test iters == length(cursor)
    end

    for empty_ds in empty_data_structures
        for ds in DBAPI.DatabaseFetcher(:columns, empty_ds, cursor)
            @test false  # should never be reached
        end
    end

    for empty_ds in empty_2d_data_structures
        for ds in DBAPI.DatabaseFetcher(:array, empty_ds, cursor)
            @test false  # should never be reached
        end
    end

    comparison_data(::Any) = row_results
    comparison_data(::Nullable) = nullable_row_results

    for filled_ds in filled_data_structures
        for (idx, ds) in enumerate(DBAPI.DatabaseFetcher(:rows, filled_ds, cursor))
            item = ds[1][1]
            @test item === comparison_data(item)[idx][1]
        end
    end

    for filled_ds in filled_data_structures
        for (idx, ds) in enumerate(DBAPI.DatabaseFetcher(:columns, filled_ds, cursor))
            item = ds[1][1]
            @test item === comparison_data(item)[idx][1]
        end
    end

    for filled_ds in filled_2d_data_structures
        for (idx, ds) in enumerate(DBAPI.DatabaseFetcher(:array, filled_ds, cursor))
            item = ds[1, 1]
            @test item === comparison_data(item)[idx][1]
            @test length(ds) == 1
        end
    end

    # bad queries
    @test_throws DBAPI.NotImplementedError DBAPI.execute!(
        cursor,
        ColumnarArrayQuery([:foo, :bar], 1:3),
        (),
    )
    @test_throws DBAPI.NotImplementedError DBAPI.executemany!(
        cursor,
        ColumnarArrayQuery([:foo, :bar], 1:3),
        ((),()),
    )
end

end

TestColumnarArrayInterface.main()
