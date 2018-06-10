--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 


------------------------------------------------------------

local LogEntry = NPL.load("./LogEntry");
------------------------------------------------------------
]] --

local LogValueType = NPL.load("./LogValueType.lua");
local LogEntry = NPL.export();

function LogEntry:new(term, value, valueType)
  local o = {
    -- byte array
    value = value, --value should not be nil?
    term = term or 0,
    valueType = valueType or LogValueType.Application
  }
  setmetatable(o, self)
  return o
end

function LogEntry:__index(name)
  return rawget(self, name) or LogEntry[name]
end

function LogEntry:__tostring()
  return util.table_tostring(self)
end
