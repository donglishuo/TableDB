--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 


------------------------------------------------------------
NPL.load("./RaftConsensus.lua");
local RaftConsensus = commonlib.gettable("Raft.RaftConsensus");
------------------------------------------------------------
]] --


local RaftServer = NPL.load("./RaftServer.lua")
local LoggerFactory = NPL.load("./LoggerFactory.lua")

local logger = LoggerFactory.getLogger("RaftConsensus")

local RaftConsensus = NPL.export()
-- RaftConsensus = {}

function RaftConsensus.run(context)
  if (context == nil) then
    logger.error("context cannot be null")
    return
  end

  local server = RaftServer:new(context)
  local messageSender = server:createMessageSender()
  context.stateMachine:start(messageSender)
  context.rpcListener:startListening(server)
  return messageSender
end

function RaftConsensus:__tostring()
  return util.table_tostring(self)
end
