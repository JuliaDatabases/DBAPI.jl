module FailedInterface

import DBAPI
using Base.Test
import Base.Collections: PriorityQueue

type BadInterface <: DBAPI.DatabaseInterface end
type BadConnection{T<:BadInterface} <: DBAPI.DatabaseConnection{T} end
type BadCursor{T<:BadInterface} <: DBAPI.DatabaseCursor{T} end
type BadQuery <: DBAPI.DatabaseQuery end

function main()
    dummy_io = IOBuffer()

    @test_throws DBAPI.NotImplementedError DBAPI.connect(BadInterface)
    @test_throws DBAPI.NotImplementedError DBAPI.connect(BadInterface, "foobar")
    @test_throws DBAPI.NotImplementedError DBAPI.connect(BadInterface, "foobar"; port=2345)

    connection = BadConnection{BadInterface}()

    @test_throws DBAPI.NotImplementedError Base.show(connection)
    @test_throws DBAPI.NotImplementedError DBAPI.commit(connection)
    @test_throws DBAPI.NotImplementedError DBAPI.isopen(connection)
    @test_throws DBAPI.NotImplementedError DBAPI.close(connection)
    @test_throws DBAPI.NotImplementedError DBAPI.isopen(connection)
    @test_throws DBAPI.NotImplementedError DBAPI.commit(connection)
    @test_throws DBAPI.NotSupportedError DBAPI.rollback(connection)
    @test_throws DBAPI.NotImplementedError DBAPI.cursor(connection)

    cursor = BadCursor{BadInterface}()

    @test_throws DBAPI.NotImplementedError DBAPI.connection(cursor)
    @test_throws DBAPI.NotImplementedError Base.show(cursor)

    @test_throws DBAPI.NotImplementedError DBAPI.rows(cursor)
    @test_throws DBAPI.NotSupportedError DBAPI.columns(cursor)

    @test_throws DBAPI.NotImplementedError DBAPI.execute!(cursor, "foobar", (1, "d"))
    @test_throws DBAPI.NotImplementedError DBAPI.executemany!(cursor, "foobar", ((1, "d"), ("6", 0xd)))

    for i in [12, :twelve]
        for j in [12, :twelve]
            @test_throws DBAPI.NotSupportedError cursor[i, j]
        end
    end

    @test_throws DBAPI.NotSupportedError cursor["far", Set([1,2,3])]

    empty_data_structures = (
        Array{Any}[Array{Any}(0)],
        Array{Any}[Array{Any}(0, 0)],
        Dict{Any, Any}[Dict{Any, Any}()],
        PriorityQueue[PriorityQueue()],
        Dict{Any,Array{Any}}(),
    )

    empty_2d_data_structures = (
        Array{Any}(0,0),
        Dict{Any, Any}(),
        PriorityQueue(),
    )

    filled_pq = PriorityQueue()
    filled_pq[1] = 1

    data_structures = (
        Array{Any}[Array{Any}(1)],
        Array{Any}[Array{Any}(1, 1)],
        Dict{Any, Any}[Dict{Any, Any}(1=>5)],
        PriorityQueue[filled_pq],
        Dict{Any,Array{Any}}(1=>Array{Any}(1)),
    )

    filled_2d_pq = PriorityQueue()
    filled_pq[1, 1] = 1

    filled_2d_data_structures = (
        Array{Any}(1, 1),
        Dict{Any, Any}((1, 1)=>5),
        filled_pq,
    )

    for ds in empty_2d_data_structures
        @test ds == DBAPI.fetchinto!(ds, cursor)
    end

    for ds in empty_data_structures
        @test ds == DBAPI.fetchrowsinto!(ds, cursor)
    end

    for ds in empty_data_structures
        @test ds == DBAPI.fetchcolumnsinto!(ds, cursor)
    end

    for ds in filled_2d_data_structures
        @test_throws DBAPI.NotSupportedError DBAPI.fetchinto!(ds, cursor)
    end

    for ds in data_structures
        @test_throws DBAPI.NotSupportedError DBAPI.fetchrowsinto!(ds, cursor)
    end

    for ds in data_structures
        @test_throws DBAPI.NotSupportedError DBAPI.fetchcolumnsinto!(ds, cursor)
    end

    # testing that these methods exist and run
    # @test_throws does not do that, unfortunately
    Base.showerror(dummy_io, DBAPI.NotImplementedError{BadInterface}())
    Base.showerror(dummy_io, DBAPI.NotSupportedError{BadInterface}())
    Base.showerror(dummy_io, DBAPI.DatabaseQueryError(BadInterface, BadQuery()))
end

end

FailedInterface.main()
