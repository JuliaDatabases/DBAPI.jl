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

    data_structures = (
        Any[Array{Any}(1)],
        Any[Array{Any}(1, 1)],
        Any[Dict{Any, Any}()],
        Any[PriorityQueue()],
        Dict{Any,Any}(1=>Any[]),
    )
end

end

FailedInterface.main()
