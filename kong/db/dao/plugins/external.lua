local pl_file = require "pl.file"
local lyaml = require "lyaml"

local logging = require "logging"

local kong = kong

local external_plugins = {}
local _servers

local function handle_server(server_def)
  if not server_def.socket then
    -- no error, just ignore
    return
  end

  if server_def.exec then
    ngx_timer_at(0, function(premature)
      if premature then
        return
      end

      local ngx_pipe = require "ngx.pipe"

      while not ngx.worker.exiting() do
        kong.log.notice("Starting " .. server_def.name or "")
        server_def.proc = assert(ngx_pipe.spawn({
          server_def.exec, table.unpack(server_def.args or {})
        }, { environ = server_def.environment }))
        server_def.proc:set_timeouts(nil, nil, nil, 0)     -- block until something actually happens

        while true do
          local ok, reason, status = server_def.proc:wait()
          if ok ~= nil or reason == "exited" then
            kong.log.notice("external pluginserver '", server_def.name, "' terminated: ", tostring(reason), " ", tostring(status))
            break
          end
        end
      end
      kong.log.notice("Exiting: go-pluginserver not respawned.")
    end)
  end

  return server_def
end

function external_plugins.manage_servers()
  if ngx.worker.id() ~= 0 then
    print("only worker #0 can manage")
    return
  end
  assert(not _servers, "don't call manage_servers() more than once")
  _servers = {}

  if not kong.configuration.external_plugins_config then
    print ("no external plugins")
    return
  end

  local content = assert(pl_file.read(kong.configuration.external_plugins_config))
  local conf = lyaml.load(content)

  print("conf! ", logging.tostring(conf))

  for i, server_def in ipairs(conf) do
    if not server_def.name then
      server_def.name = string.format("plugin server #%d", i)
    end

    local server, err = handle_server(server_def)
    if not server then
      kong.log.error(err)
    else

      servers[#servers + 1] = server
    end
  end
end

return external_plugins
