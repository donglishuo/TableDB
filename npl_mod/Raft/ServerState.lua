--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 


------------------------------------------------------------
NPL.load("(gl)npl_mod/Raft/ServerState.lua");
local ServerState = commonlib.gettable("Raft.ServerState");
------------------------------------------------------------
]] --

local ServerState = NPL.export()

function ServerState:new(t, c, v, l)
  local o = {
    term = t or 0,
    commitIndex = c or -1,
    votedFor = v or 0,
    isLeader = l
  }
  setmetatable(o, self)
  return o
end

function ServerState:__index(name)
  return rawget(self, name) or ServerState[name]
end

function ServerState:__tostring()
  return util.table_tostring(self)
end

function ServerState:increaseTerm()
  self.term = self.term + 1
end

function ServerState:setCommitIndex(commitIndex)
  if (commitIndex > self.commitIndex) then
    self.commitIndex = commitIndex
  end
end
