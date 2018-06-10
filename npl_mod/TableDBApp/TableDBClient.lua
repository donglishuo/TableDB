--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 
]] --

NPL.load("script/ide/commonlib.lua")

local LoggerFactory = NPL.load("../Raft/LoggerFactory.lua")

local logger = LoggerFactory.getLogger("TableDBClient")
local clientMode = ParaEngine.GetAppCommandLineByParam("clientMode", "appendEntries")
local serverId = tonumber(ParaEngine.GetAppCommandLineByParam("serverId", "5"))

local function executeAsClient()
  if clientMode == "appendEntries" then
    -- TestPerformance()
    -- TestBulkOperations()
    -- TestTimeout()
    -- TestBlockingAPI()
    -- TestBlockingAPILatency()
    -- TestConnect()
    -- TestRemoveIndex()
    -- TestTable()
    -- TestTableDatabase();
    -- TestRangedQuery();
    -- TestPagination()
    -- TestCompoundIndex()
    -- TestCountAPI()
    -- TestDelete()
    
    NPL.load("../TableDB/test/test_TableDatabase.lua")
    -- TestInsertThroughputNoIndex()
    TestSQLOperations()
  else
    
    local RaftSqliteStore = NPL.load("../TableDB/RaftSqliteStore.lua")
    local param = {
      baseDir = "./",
      host = "localhost",
      port = "9004",
      id = "4",
      threadName = "rtdb",
      rootFolder = "temp/test_raft_database",
      useFile = useFileStateManager
    }
    RaftSqliteStore:createRaftClient(
      param.baseDir,
      param.host,
      param.port,
      param.id,
      param.threadName,
      param.rootFolder,
      param.useFile
    )
    local raftClient = RaftSqliteStore:getRaftClient()

    if clientMode == "addServer" then
      local serverToJoin = {
        id = serverId,
        endpoint = "tcp://localhost:900" .. serverId
      }

      
      local ClusterServer = NPL.load("../Raft/ClusterServer.lua")

      raftClient:addServer(
        ClusterServer:new(serverToJoin),
        function(response, err)
          local result = (err == nil and response.accepted and "accepted") or "denied"
          logger.info("the addServer request has been %s", result)
        end
      )
    elseif clientMode == "removeServer" then
      local serverIdToRemove = serverId
      raftClient:removeServer(
        serverIdToRemove,
        function(response, err)
          local result = (err == nil and response.accepted and "accepted") or "denied"
          logger.info("the removeServer request has been %s", result)
        end
      )
    else
      logger.error("unknown client command:%s", clientMode)
    end
  end
end

executeAsClient()

NPL.this(
  function()
  end
)
