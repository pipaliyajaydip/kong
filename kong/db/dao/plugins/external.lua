local pl_file = require "pl.file"
local lyaml = require "lyaml"
local raw_log = require "ngx.errlog".raw_log

local logging = require "logging"

local ngx = ngx
local kong = kong
local ngx_INFO = ngx.INFO

local external_plugins = {}

local _servers
local _plugin_infos



--[[

Configuration

The external_plugins_config YAML file defines a list of plugin servers.  Each one
can have the following fields:

name: (required) a unique string.  Shouldn't collide with Lua packages available as `kong.plugins.<name>`
socket: (required) path of a unix domain socket to use for RPC.
exec: (optional) executable file of the server.  If omitted, the server process won't be managed by Kong.
args: (optional) a list of strings to be passed as command line arguments.
env: (optional) a {string:string} map to be passed as environment varialbes.
info_cmd: (optional) command line to request plugin info.

--]]

--[[

RPC

Each plugin server specifies a socket path to communicate.  Protocol is the same
as Go plugins.

CONSIDER:

- when a plugin server notifies a new PID, Kong should request all plugins info again.
  Should it use RPC at this time, instead of commandline?

- Should we add a new notification to ask kong to request plugin info again?

--]]

--[[

Plugin info requests

Disclaimer:  The best way to do it is to have "ListPlugins()" and "GetInfo(plugin)"
RPC methods; but Kong would like to have all the plugin schemas at initialization time,
before full cosocket is available.  At one time, we used blocking I/O to do RPC at
non-yielding phases, but was considered dangerous.  The alternative is to use
`io.popen(cmd)` to ask fot that info.


In the externel plugins configuration, the `.info_cmd` field contains a string
to be executed as a command line.  The output should be a JSON string that decodes
as an array of objects, each defining the name, priority, version and schema of one
plugin.

    [{
      "name": ... ,
      "priority": ... ,
      "version": ... ,
      "schema": ... ,
    },
    {
      ...
    },
    ...
    ]

This array should describe all plugins currently available through this server,
no matter if actually enabled in Kong's configuration or not.

--]]


local function register_plugin_info(server_def, plugin_info)
  if _plugin_infos[plugin_info.name] then
    kong.log.error(string.format("Duplicate plugin name [%s] by %s and %s",
      plugin_info.name, _plugin_infos[plugin_info.name].server_def.name, server_def.name))
    return
  end

  _plugin_infos[plugin_info.name] = {
    server_def = server_def,
    name = plugin_info.name,
    PRIORITY = plugin_info.priority,
    VERSION = plugin_info.version,
    schema = plugin_info.schema,
  }
  -- TODO: add phase closures
end

local function ask_info(server_def)
  if not server_def.info_cmd then
    kong.log.info(string.format("No info query for %s", server_def.name))
    return
  end

  local fd, err = io.popen(server_def.info_cmd)
  if not fd then
    local msg = string.format("loading plugins info from [%s]:\n", server_def.name)
    kong.log.error(msg, err)
    return
  end

  local infos_dump = fd:read("*a")
  fd:close()
  local infos = lyaml.load(infos_dump)
  if type(infos) ~= "table" then
    kong.log.error(string.format("Not a plugin info table: \n%s\n%s",
        server_def.info_cmd, infos_dump))
    return
  end

  for _, plugin_info in ipairs(infos) do
    register_plugin_info(server_def, plugin_info)
  end
end

local function load_all_infos()
  if not kong.configuration.external_plugins_config then
    kong.log.info("no external plugins")
    return
  end

  if not _plugin_infos then
    local conf = lyaml.load(assert(pl_file.read(kong.configuration.external_plugins_config)))
    _plugin_infos = {}

    for _, server_def in ipairs(conf) do
      ask_info(server_def)
    end
  end

  return _plugin_infos
end


function external_plugins.load_plugin(plugin_name)
  return load_all_infos()[plugin_name]
end

function external_plugins.load_schema(plugin_name)
  local plugin_info = external_plugins.load_plugin(plugin_name)
  return plugin_info and plugin_info.schema
end


--[[

Process management

Servers that specify an `.exec` field are launched and managed by Kong.
This is an attempt to duplicate the smallest reasonable subset of systemd.

Each process specifies executable, arguments and environment.
Stdout and stderr are joined and logged, if it dies, Kong logs the event
and respawns the server.

--]]

local function grab_logs(proc)
  while true do
    local data, err, partial = proc:stdout_read_line()
    local line = data or partial
    if line and line ~= "" then
      raw_log(ngx_INFO, "[go-pluginserver] " .. line)
    end

    if not data and err == "closed" then
      return
    end
  end
end

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
          grab_logs(server_def.proc)
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
    kong.log.notice("only worker #0 can manage")
    return
  end
  assert(not _servers, "don't call manage_servers() more than once")
  _servers = {}

  if not kong.configuration.external_plugins_config then
    kong.log.info("no external plugins")
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

      _servers[#_servers + 1] = server
    end
  end
end

return external_plugins
