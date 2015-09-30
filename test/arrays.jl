module TestColumnarArrayInterface

import DBAPI
import DBAPI.ArrayInterfaces:
    ColumnarArrayInterface,
    ColumnarArrayQuery,
    ArrayInterfaceError
import Iterators: chain
using Base.Test


function main()
    # invalid
    @test_throws ArrayInterfaceError Base.connect(ColumnarArrayInterface, [:foo], Vector[])
    @test_throws ArrayInterfaceError Base.connect(ColumnarArrayInterface, Symbol[], Vector[[1, 2, 3]])

    # empty
    connection = Base.connect(ColumnarArrayInterface, Symbol[], Vector[])
    @test isa(connection, DBAPI.DatabaseConnection)
    @test DBAPI.isopen(connection)
    @test DBAPI.commit(connection) === nothing
    @test_throws DBAPI.NotSupportedError DBAPI.rollback(connection)
    @test Base.isopen(connection)

    cursor = DBAPI.cursor(connection)
    @test isa(cursor, DBAPI.DatabaseCursor)

    @test_throws DBAPI.DatabaseQueryError DBAPI.execute!(cursor, ColumnarArrayQuery([:one], 1:0))
    @test_throws DBAPI.DatabaseQueryError DBAPI.execute!(cursor, ColumnarArrayQuery(Symbol[], 1:1))
    @test_throws DBAPI.DatabaseQueryError DBAPI.execute!(cursor, ColumnarArrayQuery([:one], 1:1))

    @test DBAPI.execute!(cursor, ColumnarArrayQuery(Symbol[], 1:0)) === nothing

    @test isempty(collect(DBAPI.rows(cursor)))
    @test isempty(collect(DBAPI.columns(cursor)))
    @test_throws BoundsError cursor[1, 1]
    @test_throws BoundsError cursor[1, :one]

    @test_throws DBAPI.NotSupportedError cursor[:one, 1]
    @test_throws DBAPI.NotSupportedError cursor[:one, :one]
    @test_throws DBAPI.NotSupportedError cursor["one", "one"]

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
    @test_throws BoundsError cursor[1, 1]
    @test_throws BoundsError cursor[1, :one]

    @test DBAPI.execute!(cursor, ColumnarArrayQuery([:foo, :bar], 1:3)) === nothing
    row_results = [
        (1, 3.0),
        (2, 2.0),
        (3, 1.0),
    ]
    @test collect(DBAPI.rows(cursor)) == row_results
    @test collect(DBAPI.columns(cursor)) == Vector[
        [1, 2, 3],
        [3.0, 2.0, 1.0],
    ]

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
