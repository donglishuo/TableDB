--[[
Title:
Author: liuluheng
Date: 2017.03.25
Desc:


------------------------------------------------------------
NPL.load("(gl)script/Raft/RaftClient.lua");
local RaftClient = commonlib.gettable("Raft.RaftClient");
------------------------------------------------------------
]]
--
NPL.load("(gl)script/Raft/Rutils.lua");
local Rutils = commonlib.gettable("Raft.Rutils");
NPL.load("(gl)script/TableDB/RaftLogEntryValue.lua");
local RaftLogEntryValue = commonlib.gettable("TableDB.RaftLogEntryValue");
NPL.load("(gl)script/Raft/LogEntry.lua");
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
local LogValueType = NPL.load("(gl)script/Raft/LogValueType.lua");
local util = commonlib.gettable("System.Compiler.lib.util")
local LogEntry = commonlib.gettable("Raft.LogEntry");
local RaftMessageType = NPL.load("(gl)script/Raft/RaftMessageType.lua");

local RaftClient = commonlib.gettable("Raft.RaftClient");

function RaftClient:new(localAddress, RequestRPC, configuration, loggerFactory)
    local o = {
        localAddress = localAddress,
        RequestRPC = RequestRPC,
        configuration = configuration,
        leaderId = configuration.servers[math.random(#configuration.servers)].id,
        randomLeader = true,
        -- connected = false,
        logger = loggerFactory.getLogger("RaftClient"),
    }
    setmetatable(o, self);
    
    NPL.StartNetServer(localAddress.host, localAddress.port);
    
    for _, server in ipairs(configuration.servers) do
        Rutils.initConnect(4, server)
    end
    
    return o;
end

function RaftClient:__index(name)
    return rawget(self, name) or RaftClient[name];
end

function RaftClient:__tostring()
    return util.table_tostring(self)
end



function RaftClient:appendEntries(values, callbackFunc)
    if values and #values == 0 then
        return
    end
    local logEntries = {}
    if type(values) == "table" then
        for i, v in ipairs(values) do
            logEntries[#logEntries + 1] = LogEntry:new(0, v)
        end
    elseif type(values) == "string" then
        logEntries[#logEntries + 1] = LogEntry:new(0, values)
    end
    
    request = {
        messageType = RaftMessageType.ClientRequest,
        logEntries = logEntries,
    }
    
    self:tryCurrentLeader(request, callbackFunc, 0, 0)
end


function RaftClient:addServer(server, callbackFunc)
    if (server == nil) then
        self.logger.error("server cannot be null");
        return
    end
    
    local logEntries = {LogEntry:new(0, server:toBytes(), LogValueType.ClusterServer)};
    local request = {
        messageType = RaftMessageType.AddServerRequest,
        logEntries = logEntries,
    }
    
    self:tryCurrentLeader(request, callbackFunc, 500, 0);
end


function RaftClient:removeServer(serverId, callbackFunc)
    if (serverId < 0) then
        self.logger.error("serverId must be equal or greater than zero");
        return
    end
    
    -- assume {serverId} is string
    local logEntries = {LogEntry:new(0, serverId, LogValueType.ClusterServer)};
    local request = {
        messageType = RaftMessageType.RemoveServerRequest,
        logEntries = logEntries,
    }
    
    self:tryCurrentLeader(request, callbackFunc, 500, 0);
end



function RaftClient:tryCurrentLeader(request, callbackFunc, rpcBackoff, retry)
    self.logger.info("trying request to %d as current leader from %s, trying %dth", self.leaderId, self.localAddress.id, retry);
    
    local this = self
    
    local backoff_retry_func = function(...)
        if this.randomLeader then
            -- try a random server as leader
            this.leaderId = this.configuration.servers[math.random(#this.configuration.servers)].id;
            this.logger.info("next should try server: %d", this.leaderId)
            this.randomLeader = true;
        end
        
        if (rpcBackoff > 0) then
            -- local backoff_timer = commonlib.Timer:new({callbackFunc = function(timer)
            --                                    this:tryCurrentLeader(request, callbackFunc, rpcBackoff + 500, retry + 1);
            --                                end})
            -- backoff_timer:Change(rpcBackoff, nil);
            ParaEngine.Sleep(rpcBackoff / 1000);
        end
        this:tryCurrentLeader(request, callbackFunc, rpcBackoff + 500, retry + 1);
    end
    
    local HandleResponse = function(err, response)
        if not err then
            this.logger.debug("response from remote server, leader: %d, accepted: %s",
                response.destination, response.accepted and "true" or "false");
            if (not response.accepted) then
                -- set the leader return from the server
                if (this.leaderId == response.destination and not this.randomLeader) then
                    -- no more retry
                    -- skip here
                else
                    if response.destination == -1 then
                        -- try a random server as leader
                        this.logger.debug("there is not a leader in the cluster, try a random one");
                        this.leaderId = this.configuration.servers[math.random(#this.configuration.servers)].id;
                        this.logger.debug("next should try server: %d", this.leaderId)
                        this.randomLeader = true;
                    else
                        this.randomLeader = false;
                        this.leaderId = response.destination;
                    end
                    
                    this.logger.debug("tried %ds, server len:%d", retry, #self.configuration.servers)
                    if (retry <= 3 * #self.configuration.servers) then
                        -- return this:tryCurrentLeader(request, callbackFunc, rpcBackoff + 5000, retry + 1);
                        return backoff_retry_func();
                    end
                end
            end
            
            if callbackFunc then
                callbackFunc(response, err)
            end
        elseif err == "timeout" then
            self.logger.debug("the request is timeout")
        -- backoff_retry_func()
        else
            self.logger.debug("rpc error, failed(%d) to send request to remote server, err:%s. tried %d, no more try here", activate_result, err, retry);
        end
    end
    
    self.HandleResponse = HandleResponse;
    
    local activate_result = self.RequestRPC(self.localAddress, "server" .. self.leaderId .. ":", request, HandleResponse, nil);
    if (activate_result ~= 0) then
        self.logger.debug("rpc error, failed(%d) to send request to remote server. tried %dth", activate_result, retry);
        if (retry > 3 * #self.configuration.servers) then
            self.logger.error("FAILED. reach to the max retry. tried %ds", retry);
            if callbackFunc then
                callbackFunc({}, "FAILED")
            end
            return;
        end
        
        backoff_retry_func()
    end
end
