__precompile__()

module DBAPI

using Reexport

include("DBAPIBase.jl")
include("arrays.jl")

@reexport using .DBAPIBase
import .DBAPIBase: NotImplementedError, NotSupportedError
import .ArrayInterfaces: ColumnarArrayInterface
export ColumnarArrayInterface

end # module
