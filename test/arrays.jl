module TestColumnarArrayInterface

using FactCheck
using DataStructures

import Iterators: chain

import DBAPI
import DBAPI.ArrayInterfaces:
    ColumnarArrayInterface,
    ColumnarArrayQuery,
    ArrayInterfaceError


facts("Array interface") do
    # invalid
    @fact_throws ArrayInterfaceError Base.connect(ColumnarArrayInterface, [:foo], Vector[])
    @fact_throws ArrayInterfaceError Base.connect(identity, ColumnarArrayInterface, [:foo], Vector[])
    @fact_throws ArrayInterfaceError Base.connect(ColumnarArrayInterface, Symbol[], Vector[[1, 2, 3]])
    @fact_throws ArrayInterfaceError Base.connect(ColumnarArrayInterface, [:foo, :bar], Vector[[1], [2, 3]])

    # do block (without the do block)
    connection = Base.connect(identity, ColumnarArrayInterface, Symbol[], Vector[])
    @fact isa(connection, DBAPI.DatabaseConnection) --> true
    @fact DBAPI.isopen(connection) --> false

    # empty
    connection = Base.connect(ColumnarArrayInterface, Symbol[], Vector[])
    @fact isa(connection, DBAPI.DatabaseConnection) --> true
    @fact DBAPI.isopen(connection) --> true
    @fact DBAPI.commit(connection) --> exactly(nothing)
    @fact_throws DBAPI.NotSupportedError DBAPI.rollback(connection)
    @fact Base.isopen(connection) --> true

    cursor = DBAPI.cursor(connection)
    @fact isa(cursor, DBAPI.DatabaseCursor) --> true
    @fact isa(cursor, DBAPI.FixedLengthDatabaseCursor) --> true

    @fact_throws ArrayInterfaceError DBAPI.rows(cursor)
    @fact_throws ArrayInterfaceError DBAPI.columns(cursor)

    @fact_throws DBAPI.DatabaseQueryError DBAPI.execute!(cursor, ColumnarArrayQuery([:one], 1:0))
    @fact_throws DBAPI.DatabaseQueryError DBAPI.execute!(cursor, ColumnarArrayQuery(Symbol[], 1:1))
    @fact_throws DBAPI.DatabaseQueryError DBAPI.execute!(cursor, ColumnarArrayQuery([:one], 1:1))

    @fact DBAPI.execute!(cursor, ColumnarArrayQuery(Symbol[], 1:0)) --> exactly(nothing)

    @fact isempty(collect(DBAPI.rows(cursor))) --> true
    @fact isempty(collect(DBAPI.columns(cursor))) --> true
    @fact_throws BoundsError cursor[1, 1]
    @fact_throws BoundsError cursor[1, :one]
    @fact isempty(cursor) --> true
    @fact length(cursor) --> 0

    @fact_throws DBAPI.NotImplementedError cursor[:one, 1]
    @fact_throws DBAPI.NotImplementedError cursor[:one, :one]
    @fact_throws DBAPI.NotImplementedError cursor["one", "one"]

    @fact Base.close(connection) --> exactly(nothing)
    @fact Base.isopen(connection) --> false

    # non-empty
    connection = Base.connect(
        ColumnarArrayInterface,
        [:foo, :bar],
        Vector[[1, 2, 3], [3.0, 2.0, 1.0]]
    )
    @fact isa(connection, DBAPI.DatabaseConnection) --> true
    @fact DBAPI.isopen(connection) --> true
    @fact DBAPI.commit(connection) --> exactly(nothing)
    @fact_throws DBAPI.NotSupportedError DBAPI.rollback(connection)
    @fact Base.isopen(connection) --> true

    cursor = DBAPI.cursor(connection)
    @fact isa(cursor, DBAPI.DatabaseCursor) --> true
    @fact isa(cursor, DBAPI.FixedLengthDatabaseCursor) --> true

    @fact_throws DBAPI.DatabaseQueryError DBAPI.execute!(cursor, ColumnarArrayQuery([:one], 1:1))

    @fact DBAPI.execute!(cursor, ColumnarArrayQuery(Symbol[], 1:0)) --> exactly(nothing)

    @fact collect(DBAPI.rows(cursor)) --> isempty
    @fact collect(DBAPI.columns(cursor)) --> isempty
    @fact cursor --> isempty
    @fact length(cursor) --> 0
    @fact_throws BoundsError cursor[1, 1]
    @fact_throws BoundsError cursor[1, :one]

    @fact DBAPI.execute!(cursor, ColumnarArrayQuery([:foo, :bar], 1:3)) --> exactly(nothing)
    row_results = [
        (1, 3.0),
        (2, 2.0),
        (3, 1.0),
    ]
    nullable_row_results = map(x -> map(Nullable{Any}, x), row_results)
    @fact collect(DBAPI.rows(cursor)) --> row_results
    @fact collect(DBAPI.columns(cursor)) --> Vector[
        [1, 2, 3],
        [3.0, 2.0, 1.0],
    ]

    @fact cursor[3, :bar] --> 1.0
    @fact cursor[1, :foo] --> 1
    @fact_throws BoundsError cursor[4, :foo]
    @fact_throws BoundsError cursor[1, :one]
    @fact_throws BoundsError cursor[0, :one]

    @fact cursor --> not(isempty)
    @fact length(cursor) --> length(row_results)

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
        @fact DBAPI.fetchintorows!(ds, cursor) --> (ds, 1)
    end

    for ds in empty_data_structures
        @fact DBAPI.fetchintocolumns!(ds, cursor) --> exactly((ds, 0))
    end

    for ds in empty_2d_data_structures
        @fact DBAPI.fetchintoarray!(ds, cursor) --> exactly((ds, 0))
    end

    for ds in filled_data_structures
        @fact DBAPI.fetchintorows!(ds, cursor) --> exactly((ds, 1))
    end

    for ds in filled_data_structures
        @fact DBAPI.fetchintocolumns!(ds, cursor) --> exactly((ds, 1))
    end

    for ds in filled_2d_data_structures
        @fact DBAPI.fetchintoarray!(ds, cursor) --> exactly((ds, 1))
    end

    first_empty(a::Associative) = isempty(first(values(a)))
    first_empty(a) = isempty(first(a))

    for empty_ds in empty_data_structures
        iters = 0
        for ds in DBAPI.DatabaseFetcher(:rows, empty_ds, cursor)
            # ds will be a collection containing an empty row
            @fact first_empty(ds) --> true
            iters += 1
        end
        @fact iters --> length(cursor)
    end

    for empty_ds in empty_data_structures
        for ds in DBAPI.DatabaseFetcher(:columns, empty_ds, cursor)
            @fact true --> false  # should never be reached
        end
    end

    for empty_ds in empty_2d_data_structures
        for ds in DBAPI.DatabaseFetcher(:array, empty_ds, cursor)
            @fact true --> false  # should never be reached
        end
    end

    comparison_data(::Any) = row_results
    comparison_data(::Nullable) = nullable_row_results

    for filled_ds in filled_data_structures
        for (idx, ds) in enumerate(DBAPI.DatabaseFetcher(:rows, filled_ds, cursor))
            item = ds[1][1]
            @fact item --> exactly(comparison_data(item)[idx][1])
        end
    end

    for filled_ds in filled_data_structures
        for (idx, ds) in enumerate(DBAPI.DatabaseFetcher(:columns, filled_ds, cursor))
            item = ds[1][1]
            @fact item --> exactly(comparison_data(item)[idx][1])
        end
    end

    for filled_ds in filled_2d_data_structures
        for (idx, ds) in enumerate(DBAPI.DatabaseFetcher(:array, filled_ds, cursor))
            item = ds[1, 1]
            @fact item --> exactly(comparison_data(item)[idx][1])
            @fact length(ds) --> 1
        end
    end

    # nullable db data
    connection = Base.connect(
        ColumnarArrayInterface,
        [:foo, :bar],
        Vector[[Nullable(1), Nullable(2), Nullable(3)], [3.0, 2.0, 1.0]]
    )
    @fact isa(connection, DBAPI.DatabaseConnection) --> true
    @fact DBAPI.isopen(connection) --> true
    @fact DBAPI.commit(connection) --> exactly(nothing)
    @fact_throws DBAPI.NotSupportedError DBAPI.rollback(connection)
    @fact Base.isopen(connection) --> true

    cursor = DBAPI.cursor(connection)
    @fact isa(cursor, DBAPI.DatabaseCursor) --> true
    @fact isa(cursor, DBAPI.FixedLengthDatabaseCursor) --> true

    @fact_throws DBAPI.DatabaseQueryError DBAPI.execute!(cursor, ColumnarArrayQuery([:one], 1:1))

    @fact DBAPI.execute!(cursor, ColumnarArrayQuery(Symbol[], 1:0)) --> exactly(nothing)

    @fact collect(DBAPI.rows(cursor)) --> isempty
    @fact collect(DBAPI.columns(cursor)) --> isempty
    @fact cursor --> isempty
    @fact length(cursor) --> 0
    @fact_throws BoundsError cursor[1, 1]
    @fact_throws BoundsError cursor[1, :one]

    @fact DBAPI.execute!(cursor, ColumnarArrayQuery([:foo, :bar], 2:3)) --> exactly(nothing)
    row_results = Any[
        (Nullable(2), 2.0),
        (Nullable(3), 1.0),
    ]
    nullable_row_results = map(x -> map(y -> isa(y, Nullable) ? y : Nullable{Any}(y), x), row_results)
    @fact isequal(collect(DBAPI.rows(cursor)), row_results) --> true
    @fact isequal(collect(DBAPI.columns(cursor)), Vector[
        [Nullable(2), Nullable(3)],
        [2.0, 1.0],
    ]) --> true

    # unsupported query types
    @fact_throws DBAPI.NotSupportedError DBAPI.execute!(
        cursor,
        "[:foo, :bar], 1:3",
    )
    @fact_throws DBAPI.NotSupportedError DBAPI.execute!(
        cursor,
        "[:foo, :bar], 1:3",
        (),
    )
    @fact_throws DBAPI.NotSupportedError DBAPI.execute!(
        cursor,
        DBAPI.StringMultiparameterQuery(
            "[:foo, :bar], 1:3",
            ((),())
        ),
    )
end

end
