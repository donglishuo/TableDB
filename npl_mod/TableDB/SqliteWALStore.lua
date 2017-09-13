--[[
Title: base class for store
Author(s): liuluheng,
Date: 2017/7/31
Desc:
use the lib:
------------------------------------------------------------
NPL.load("(gl)npl_mod/TableDB/SqliteWALStore.lua");
local SqliteWALStore = commonlib.gettable("TableDB.SqliteWALStore");
------------------------------------------------------------
]]
NPL.load("(gl)script/ide/commonlib.lua");
NPL.load("(gl)script/sqlite/sqlite3.lua");

NPL.load("(gl)script/ide/System/Database/SqliteStore.lua");
local SqliteStore = commonlib.gettable("System.Database.SqliteStore");
local SqliteWALStore = commonlib.inherit(SqliteStore, commonlib.gettable("TableDB.SqliteWALStore"));
local LoggerFactory = NPL.load("(gl)npl_mod/Raft/LoggerFactory.lua");

local cbWALHandlerFile = "(%s)RPC/WALHandler.lua";
local cb_thread = "raft"

SqliteWALStore.logger = LoggerFactory.getLogger("SqliteWALStore");


function SqliteWALStore:ctor()
end

function SqliteWALStore:init(collection, init_args)
    SqliteWALStore._super.init(self, collection);
    local dbName = self.kFileName
    self._db:set_wal_page_hook(function(page_data, pgno, nTruncate, isCommit)
        local msg = {
            rootFolder = collection:GetParent():GetRootFolder(),
            collectionName = collection:GetName(),
            page_data = page_data,
            pgno = pgno,
            nTruncate = nTruncate,
            isCommit = isCommit,
        }
        
        NPL.activate(self:GetReplyAddress(cb_thread or "main"), msg);
        self.logger.trace("pgSize %d, pgno %d, nTruncate %d, isCommit %d", #page_data, pgno, nTruncate, isCommit);
        return 1
    end)

    return self;
end

function SqliteWALStore:injectWALPage(query, callbackFunc)
    local r = self._db:wal_inject_page(query.page_data, query.pgno, query.nTruncate, query.isCommit)
    if r ~= 0 then
      self.logger.error("%d inject failed, %d", query.logIndex, r);
    else
      self.logger.trace("injected %d", query.logIndex);
    end
    if (not self.checkpoint_timer:IsEnabled()) then
        self.checkpoint_timer:Change(self.AutoCheckPointInterval, self.AutoCheckPointInterval);
    end
end


function SqliteWALStore:Close()
    SqliteWALStore._super.Close(self)
end


function SqliteWALStore:GetReplyAddress(cb_thread)
    return format(cbWALHandlerFile, cb_thread);
end
