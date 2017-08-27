-- Copyright (C) Anjia (anjia0532)

local redis_c = require("resty.redis")

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local _M = new_tab(0, 54)

_M._VERSION = '0.02'

local mt = {__index = _M}

-- if res is ngx.null or nil or type(res) is table and all value is ngx.null return true else false
local function _is_null(res)
  if type(res) == "table" then
    for _, v in pairs(res) do
      if v ~= ngx.null then
        return false
      end
    end
  elseif res == ngx.null or res ==nil then
    return true
  end

  return false
end


-- encapsulation redis connect
local function _connect_mod(self,redis)
  -- set timeout
  redis:set_timeout(self._opts.timeout)
  
  -- set redis host,port
  local ok, err = redis:connect(self._opts.host, self._opts.port)
  if not ok or err then
    return nil, err
  end
  
  -- set auth
  if self._opts.password then
    local times, err = redis:get_reused_times()
    if times == 0 then
      local ok, err = redis:auth(self._opts.password)
      if not ok or err then
        return nil, err
      end
    elseif err then
      return nil, err
    end
  end

  return redis, nil
end

-- put it into the connection pool of size (default 100), with max idle time (default 60s)
local function _set_keepalive_mod(self,redis )
  return redis:set_keepalive(self._opts.keepalive, self._opts.pool_size)
end

-- encapsulation subscribe
function _M.subscribe( self, channel )

  -- init redis
  local redis, err = redis_c:new()
  if not redis then
    return nil, err
  end

  -- get connect
  local ok, err = _connect_mod(self,redis)
  if not ok or err then
    return nil, err
  end

  -- sub channel
  local res, err = redis:subscribe(channel)
  if not res then
      return nil, err
  end

  local function do_read_func ( do_read )
    if do_read == nil or do_read == true then
        res, err = redis:read_reply()
        if not res then
            return nil, err
        end
        return res
    end
    
    -- if do_read is false 
    redis:unsubscribe(channel)
    _set_keepalive_mod(self,redis)
    return
  end

  return do_read_func
end

-- init pipeline,default cmds num is 4
function _M.init_pipeline(self, n)
  self._reqs = new_tab(n or 4, 0)
end

-- cancel pipeline
function _M.cancel_pipeline(self)
  self._reqs = nil
end

-- commit pipeline
function _M.commit_pipeline(self)
  -- get cache cmds
  local _reqs = rawget(self, "_reqs") 
  if not _reqs then
    return nil, "no pipeline"
  end

  self._reqs = nil

  -- init redis
  local redis, err = redis_c:new()
  if not redis then
    return nil, err
  end

  -- get connect
  local ok, err = _connect_mod(self,redis)
  if not ok or err then
    return nil, err
  end

  redis:init_pipeline()
  
   --redis command like set/get ...
  for _, vals in ipairs(_reqs) do
    -- vals[1] is redis cmd
    local fun = redis[vals[1]]
    -- get params without cmd
    table.remove(vals , 1)
    -- invoke redis cmd 
    fun(redis, unpack(vals))
  end

  -- commit pipeline
  local results, err = redis:commit_pipeline()
  if not results or err then
    return {}, err
  end

  -- check null
  if _is_null(results) then
    results = {}
    ngx.log(ngx.WARN, "redis result is null")
  end

  -- put it into the connection pool
  _set_keepalive_mod(self,redis)

  -- if null set default value nil
  for i,value in ipairs(results) do
    if _is_null(value) then
      results[i] = nil
    end
  end

  return results, err
end

-- common method
local function do_command(self, cmd, ...)
  
  -- pipeline reqs
  local _reqs = rawget(self, "_reqs")
  if _reqs then
    -- append reqs
    _reqs[#_reqs + 1] = {cmd,...}
    return
  end
  
  local redis, err = redis_c:new()
  if not redis then
    return nil, err
  end

  local ok, err = _connect_mod(self,redis)
  if not ok or err then
    return nil, err
  end

  -- exec redis cmd
  local method = redis[cmd]
  local result, err = method(redis, ...)
  if not result or err then
    return nil, err
  end

  -- check null
  if _is_null(result) then
    result = nil
  end

  -- put it into the connection pool
  local ok, err = _set_keepalive_mod(self,redis)
  if not ok or err then
    return nil, err
  end

  return result, nil
end

-- init options
function _M.new(self, opts)
  opts=opts or {}
  opts={
      host = opts.host or '127.0.0.1',
      port = opts.port or 6379,
      db_index = opts.db_index or 0,
      password = opts.password or nil,
      timeout = opts.timeout or 1000,
      keepalive = opts.keepalive or 60000, --60s
      pool_size = opts.pool_size or 100,
      _reqs = nil
  }
  
  return setmetatable({_opts=opts}, mt)
end

-- dynamic cmd
setmetatable(_M, {__index = function(self, cmd)
    local method =
        function (self, ...)
            return do_command(self, cmd, ...)
        end

  -- cache the lazily generated method in our
  -- module table
    _M[cmd] = method
    return method
end})

return _M
