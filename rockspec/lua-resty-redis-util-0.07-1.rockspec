rockspec_format = "3.0"
package = "lua-resty-redis-util"
version = "0.07-1"
supported_platforms = {"linux", "macosx"}

source = {
   url = "git://github.com/anjia0532/lua-resty-redis-util",
   tag = "v0.07"
}

description = {
   summary = "openresty/lua-resty-redis 封装工具类",
   detailed = [[
      本项目是基于openresty/lua-resty-redis 是章亦春（agentzh）开发的openresty中的操作redis的库。进行二次封装的工具库。核心功能还是由openresty/lua-resty-redis完成的。
   ]],
   homepage = "https://github.com/anjia0532/lua-resty-redis-util",
   license = "BSD",
   labels = { "openresty" , "redis"}
}
dependencies = {
}

build = {
  type = "builtin",
  modules = {
    ["resty.redis-util"] = "lib/resty/redis-util.lua",
  }
}
