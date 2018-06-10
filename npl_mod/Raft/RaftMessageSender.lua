--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 
  call RaftServer to process msgs

------------------------------------------------------------
NPL.load("./RaftMessageSender.lua");
local RaftMessageSender = commonlib.gettable("Raft.RaftMessageSender");
------------------------------------------------------------
]] --

NPL.load("(gl)script/ide/System/Compiler/lib/util.lua")
local util = commonlib.gettable("System.Compiler.lib.util")

local LoggerFactory = NPL.load("./LoggerFactory.lua")
local Rpc = NPL.load("./Rpc.lua")
local RaftMessageType = NPL.load("./RaftMessageType.lua")

local logger = LoggerFactory.getLogger("RaftMessageSender")

local RaftMessageSender = NPL.export()

function RaftMessageSender:new(server)
  local o = {
    server = server,
    logger = logger
  }
  setmetatable(o, self)
  return o
end

function RaftMessageSender:__index(name)
  return rawget(self, name) or RaftMessageSender[name]
end

function RaftMessageSender:__tostring()
  return util.table_tostring(self)
end

function RaftMessageSender:appendEntries(values)
  return self:sendMessageToLeader(values)
end

function RaftMessageSender:sendMessageToLeader(request)
  local leaderId = self.server.leader
  local config = self.server.config

  local response = {
    messageType = RaftMessageType.AppendEntriesResponse,
    destination = leaderId,
    accepted = false
  }

  if (leaderId == -1) then
    self.logger.error("no leader in the cluster now")
    return response
  end

  if (leaderId == self.server.id) then
    return self.server:processRequest(request)
  else
    return response
  end

  -- should we forward to the leader ?
  if
    (RaftRequestRPC(
      "server" .. self.server.id .. ":",
      "server" .. leaderId .. ":",
      request,
      function(err, msg)
        --    o:resumeHeartbeatingSpeed();

        if callbackFunc then
          callbackFunc(msg)
        end
      end
    ) ~= 0)
   then
  -- self:slowDownHeartbeating()
  end
end
