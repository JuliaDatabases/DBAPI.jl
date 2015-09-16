module FailedInterface
import DBAPI
using Base.Test
import Base.Collections: PriorityQueue

type BadInterface <: DBAPI.DatabaseInterface end
type BadConnection{T<:BadInterface} <: DBAPI.DatabaseConnection{T} end
type BadCursor{T<:BadInterface} <: DBAPI.DatabaseCursor{T} end

function main()
    @test_throws DBAPI.NotImplementedError DBAPI.connect(BadInterface)
    @test_throws DBAPI.NotImplementedError DBAPI.connect(BadInterface, "foobar")
    @test_throws DBAPI.NotImplementedError DBAPI.connect(BadInterface, "foobar"; port=2345)

    conn = BadConnection{BadInterface}()

    @test_throws DBAPI.NotImplementedError DBAPI.close(conn)
    @test_throws DBAPI.NotImplementedError DBAPI.commit(conn)
    @test_throws DBAPI.NotSupportedError DBAPI.rollback(conn)
    @test_throws DBAPI.NotImplementedError DBAPI.cursor(conn)

    cursor = BadCursor{BadInterface}()

    @test_throws DBAPI.NotImplementedError DBAPI.rows(cursor, Inf)
    @test_throws DBAPI.NotImplementedError DBAPI.rows(cursor, 1)
    @test_throws DBAPI.NotImplementedError DBAPI.rows(cursor)
    @test_throws DBAPI.NotSupportedError DBAPI.columns(cursor)

    for i in [12, :twelve]
        for j in [12, :twelve]
            @test_throws DBAPI.NotSupportedError cursor[i, j]
        end
    end

    empty_data_structures = (
        Array{Any}[Array{Any}(0)],
        Array{Any}[Array{Any}(0, 0)],
        Dict{Any, Any}[Dict{Any, Any}()],
        PriorityQueue[PriorityQueue()],
        Dict{Any,Array{Any}}(),
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

    for ds in empty_data_structures
        @test ds == DBAPI.fetchinto!(ds, cursor)
    end

    for ds in data_structures
        @test_throws DBAPI.NotSupportedError DBAPI.fetchinto!(ds, cursor)
    end
end

end

FailedInterface.main()
