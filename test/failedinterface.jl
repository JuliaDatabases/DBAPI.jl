type BadInterface <: DBAPI.AbstractDatabaseInterface end
type BadConnection{BadInterface} <: DBAPI.AbstractDatabaseConnection{BadInterface} end

function failedinterface()
    @test_throws DBAPI.NotImplementedError DBAPI.connect(BadInterface)
    @test_throws DBAPI.NotImplementedError DBAPI.connect(BadInterface, "foobar")
    @test_throws DBAPI.NotImplementedError DBAPI.connect(BadInterface, "foobar"; port=2345)

    conn = BadConnection{BadInterface}()

    @test_throws DBAPI.NotImplementedError DBAPI.close(conn)
    @test_throws DBAPI.NotImplementedError DBAPI.commit(conn)
    @test_throws DBAPI.NotSupportedError DBAPI.rollback(conn)
    @test_throws DBAPI.NotImplementedError DBAPI.cursor(conn)
end

failedinterface()