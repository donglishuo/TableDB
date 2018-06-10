--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 


------------------------------------------------------------
local LoggerFactory = NPL.load("(gl)npl_mod/Raft/LoggerFactory.lua");
------------------------------------------------------------
]] --

local Logger = NPL.load("./Logger")

local LoggerFactory = NPL.export()

function LoggerFactory.getLogger(modname)
  return Logger:new(modname)
end
