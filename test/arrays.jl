module TestColumnarArrayInterface

import DBAPI
import DBAPI.ArrayInterfaces: ColumnarArrayInterface
using Base.Test


function main()
    # empty
    connection = DBAPI.connect(ColumnarArrayInterface, Symbol[], Vector[])
    @test isa(connection, DBAPI.DatabaseConnection)
    @test DBAPI.isopen(connection)
    @test DBAPI.commit(connection) === nothing
    @test_throws DBAPI.NotSupportedError DBAPI.rollback(connection)
    @test DBAPI.isopen(connection)

    cursor = DBAPI.cursor(connection)
    @test isa(cursor, DBAPI.DatabaseCursor)

    @test DBAPI.close(connection) == nothing
    @test DBAPI.isopen(connection) == false
end

end

TestColumnarArrayInterface.main()
