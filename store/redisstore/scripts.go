package redisstore

import _ "embed"

//go:embed lua/trim.lua
var trimLuaSource string

//go:embed lua/enqueue.lua
var enqueueLuaBody string

//go:embed lua/cancel.lua
var cancelLuaBody string
