--[[
Title:
Author: liuluheng
Date: 2017.04.12
Desc:

------------------------------------------------------------
NPL.load("./TableDB/SQLHandler.lua");
local SQLHandler = commonlib.gettable("TableDB.SQLHandler");
------------------------------------------------------------
]]
--

NPL.load("(gl)script/ide/commonlib.lua")
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua")
local util = commonlib.gettable("System.Compiler.lib.util")
NPL.load("(gl)script/ide/System/Database/TableDatabase.lua")
local TableDatabase = commonlib.gettable("System.Database.TableDatabase")

local Rpc = NPL.load("../Raft/Rpc.lua")
local RaftLogEntryValue = NPL.load("./RaftLogEntryValue.lua")
local ServerState = NPL.load("../Raft/ServerState.lua")
local RaftMessageType = NPL.load("../Raft/RaftMessageType.lua")
local LoggerFactory = NPL.load("../Raft/LoggerFactory.lua")
local SQLHandler = NPL.export()

local g_threadName = __rts__:GetName()

local response = {
  messageType = RaftMessageType.AppendEntriesResponse
}

-- for function not in class
local logger = LoggerFactory.getLogger("SQLHandler")

function SQLHandler:new(baseDir, useFile)
  local ServerStateManager
  if useFile then
    local FileBasedServerStateManager = NPL.load("../Raft/FileBasedServerStateManager.lua")
    ServerStateManager = FileBasedServerStateManager
  else
    local SqliteBasedServerStateManager = NPL.load("../Raft/SqliteBasedServerStateManager.lua")
    ServerStateManager = SqliteBasedServerStateManager
  end

  local o = {
    baseDir = baseDir,
    stateManager = ServerStateManager:new(baseDir),
    monitorPeriod = 50000,
    threadName = g_threadName,
    logger = LoggerFactory.getLogger("SQLHandler"),
    -- record latest command for each client
    latestCommand = {},
    collections = {}
  }

  NPL.load("(gl)script/ide/timer.lua")
  o.mytimer =
    commonlib.Timer:new(
    {
      callbackFunc = function(timer)
        o.logger.debug("reading server state")
        o.state = o.stateManager:readState()
      end
    }
  )
  o.mytimer:Change(o.monitorPeriod, o.monitorPeriod)

  setmetatable(o, self)

  return o
end

function SQLHandler:__index(name)
  return rawget(self, name) or SQLHandler[name]
end

function SQLHandler:__tostring()
  return util.table_tostring(self)
end

function SQLHandler:start()
  __rts__:SetMsgQueueSize(1000000)
  local this = self
  Rpc:new():init("RaftRequestRPCInit")
  RaftRequestRPCInit.remoteThread = self.threadName
  RaftRequestRPCInit:MakePublic()
  Rpc:new():init(
    "RTDBRequestRPC",
    function(self, msg)
      msg = this:processMessage(msg)
      return msg
    end
  )
  RTDBRequestRPC.remoteThread = self.threadName
  RTDBRequestRPC:MakePublic()

  self.db = TableDatabase:new()

  self.logger.info("SQLHandler started")
end

function SQLHandler:processMessage(request)
  response.source = self.stateManager.serverId

  -- this is ad-hoc and fragile for leader re-election
  -- but it will hurt the performance if we read file everytime
  if not (self.state and self.state.isLeader) then
    self.logger.debug("reading server state")
    self.state = self.stateManager:readState()
    if not (self.state and self.state.isLeader) then
      self.logger.error("I'm not a Leader")
      response.accepted = false
      response.destination = -1
      response.term = -1
      return response
    end
  end

  response.destination = self.state.votedFor -- use destination to indicate the leadId
  response.term = self.state.term

  if self.state.votedFor ~= self.stateManager.serverId then
    response.accepted = false
    return response
  end

  -- the leader executes the SQL, but the followers just append to WAL
  if request.logEntries and #request.logEntries > 0 then
    for _, v in ipairs(request.logEntries) do
      self:handle(v.value)
    end
  end

  response.accepted = true
  return response
end

function SQLHandler:handle(data, callbackFunc)
  -- data is logEntry.value
  local raftLogEntryValue = RaftLogEntryValue:fromBytes(data)

  self.logger.trace("SQL:")
  self.logger.trace(raftLogEntryValue)

  local this = self
  local cbFunc = function(err, data, re_exec)
    local msg = {
      err = err,
      data = data,
      cb_index = raftLogEntryValue.cb_index
    }

    this.logger.trace("Result:")
    this.logger.trace(msg)

    if not re_exec then
      this.latestError = err
      this.latestData = data
    end

    RTDBRequestRPC(self.stateManager.serverId, raftLogEntryValue.serverId, msg, nil, raftLogEntryValue.callbackThread)
  end

  -- for test
  -- if callbackFunc then
  --     cbFunc = callbackFunc;
  -- end

  if tonumber(raftLogEntryValue.cb_index) <= (self.latestCommand[raftLogEntryValue.client_uid] or -2) then
    self.logger.info(
      "got a retry msg, %d <= %d",
      raftLogEntryValue.cb_index,
      self.latestCommand[raftLogEntryValue.client_uid]
    )
    cbFunc(this.latestError, this.latestData, true)
    return
  end

  -- TODO: handle leader transfer
  local config_path = raftLogEntryValue.db .. "/tabledb.config.xml"
  if raftLogEntryValue.query_type == "connect" then
    -- raftLogEntryValue.db is nil when query_type is connect
    -- we should create tabledb.config.xml here and make the storageProvider to SqliteWALStore
    self:createSqliteWALStoreConfig(raftLogEntryValue.db)
    self.db:connect(raftLogEntryValue.db, cbFunc)
  else
    if raftLogEntryValue.enableSyncMode then
      self.db:EnableSyncMode(true)
    end
    local collection = self.db[raftLogEntryValue.collectionName]

    --add to collections
    if not self.collections[raftLogEntryValue.collectionName] then
      local collectionPath = raftLogEntryValue.db .. raftLogEntryValue.collectionName
      self.logger.trace("add collection %s->%s", raftLogEntryValue.collectionName, collectionPath)
      self.collections[raftLogEntryValue.collectionName] = collectionPath
    end

    -- NOTE: this may not work when the query field named "update" or "replacement"
    if raftLogEntryValue.query.update or raftLogEntryValue.query.replacement then
      if raftLogEntryValue.enableSyncMode then
        cbFunc(
          collection[raftLogEntryValue.query_type](
            collection,
            raftLogEntryValue.query.query,
            raftLogEntryValue.query.update or raftLogEntryValue.query.replacement
          )
        )
      else
        collection[raftLogEntryValue.query_type](
          collection,
          raftLogEntryValue.query.query,
          raftLogEntryValue.query.update or raftLogEntryValue.query.replacement,
          cbFunc
        )
      end
    else
      if raftLogEntryValue.enableSyncMode then
        cbFunc(collection[raftLogEntryValue.query_type](collection, raftLogEntryValue.query))
      else
        collection[raftLogEntryValue.query_type](collection, raftLogEntryValue.query, cbFunc)
      end
    end
  end

  self.latestCommand[raftLogEntryValue.client_uid] = tonumber(raftLogEntryValue.cb_index)
end

function SQLHandler:exit(code)
  -- frustrating
  -- C/C++ API call is counted as one instruction, so Exit does not block
  ParaGlobal.Exit(code)
  -- we don't want to go more
  ParaEngine.Sleep(1)
end

function SQLHandler:createSqliteWALStoreConfig(rootFolder)
  NPL.load("(gl)script/ide/commonlib.lua")
  NPL.load("(gl)script/ide/LuaXML.lua")
  local config = {
    name = "tabledb",
    {
      name = "providers",
      {
        name = "provider",
        attr = {name = "sqliteWAL", type = "TableDB.SqliteWALStore", file = "(g1)npl_mod/TableDB/SqliteWALStore.lua"},
        ""
      }
    },
    {
      name = "tables",
      {name = "table", attr = {provider = "sqliteWAL", name = "default"}}
    }
  }

  local config_path = rootFolder .. "/tabledb.config.xml"
  local str = commonlib.Lua2XmlString(config, true)
  ParaIO.CreateDirectory(config_path)
  local file = ParaIO.open(config_path, "w")
  if (file:IsValid()) then
    file:WriteString(str)
    file:close()
  end
end

local started = false
local function activate()
  if (not started and msg and msg.start) then
    local sqlHandler = SQLHandler:new(msg.baseDir, msg.useFile)
    sqlHandler:start()
    started = true
  end
end

NPL.this(activate)
