--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/Raft/SnapshotSyncContext.lua");
local SnapshotSyncContext = commonlib.gettable("Raft.SnapshotSyncContext");
------------------------------------------------------------
]]--


local SnapshotSyncContext = commonlib.gettable("Raft.SnapshotSyncContext");

function SnapshotSyncContext:new(snapshot) 
    local o = {
        snapshot = snapshot,
        offset = 0,
    };
    setmetatable(o, self);
    return o;
end

function SnapshotSyncContext:__index(name)
    return rawget(self, name) or SnapshotSyncContext[name];
end

function SnapshotSyncContext:__tostring()
    return util.table_tostring(self)
end