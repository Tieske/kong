local plugins_schema = require "kong.dao.schemas.plugins"
local query_builder = require "kong.dao.cassandra.query_builder"
local constants = require "kong.constants"
local BaseDao = require "kong.dao.cassandra.base_dao"
local cjson = require "cjson"
local utils = require "kong.tools.utils"
local error_types = constants.DATABASE_ERROR_TYPES
local DaoError = require "kong.dao.error"

local Plugins = BaseDao:extend()

-- The Plugins dao does not have a single schema, it has multiple.
-- each plugin can have its own subschema and that needs to be accounted for.
-- So the dao will maintain a list of schema's and whenever updating data
-- that needs validating, it will first set the correct schema.


-- creates a new plugin schema and stores it in the schema list
local function new_plugin_schema(plugin_name)
  local schema = utils.deep_copy(plugins_schema)
  if plugin_name then
    local loaded, plugin_specific_schema = utils.load_module_if_exists("kong.plugins."..plugin_name..".schema")
    if not loaded then
      return nil, 'Plugin "'..tostring(plugin_name)..'" not found'
    end
    schema.fields.config.schema = plugin_specific_schema
  end
  return schema
end

function Plugins:new(properties)
  self._table = "plugins"
  self._schema = utils.deep_copy(plugins_schema)
  self._plugin_schemas = {}   -- will hold a specific schema for each plugin, indexed by plugin name
  Plugins.super.new(self, properties)
end

-- sets the current dao schema to the specific plugin schema and
-- loads the schema if not yet available
function Plugins:set_specific_schema(plugin_name)
  if not plugin_name then 
    return false, DaoError(utils.add_error(nil, "name", "name is required"), error_types.SCHEMA)
  end
  
  self._schema = self._plugin_schemas[plugin_name]
  if (not self._schema) and plugin_name then
    local err
    self._schema, err = new_plugin_schema(plugin_name)
    if err then
      return false, DaoError(err, constants.DATABASE_ERROR_TYPES.SCHEMA)
    end
    self._plugin_schemas[plugin_name] = self._schema
  end
  
  return true
end

-- @override
function Plugins:_marshall(t)
  if type(t.config) == "table" then
    t.config = cjson.encode(t.config)
  end

  return t
end

-- @override
function Plugins:_unmarshall(t)
  -- deserialize configs (tables) string to json
  if type(t.config) == "string" then
    t.config = cjson.decode(t.config)
  end
  -- remove consumer_id if null uuid
  if t.consumer_id == constants.DATABASE_NULL_ID then
    t.consumer_id = nil
  end

  return t
end

-- @override
function Plugins:update(t, full)
  local success, err = self:set_specific_schema(t.name)
  if not success then return success, err end
  
  if not t.consumer_id then
    t.consumer_id = constants.DATABASE_NULL_ID
  end
  return Plugins.super.update(self, t, full)
end

-- @override
function Plugins:insert(t)
  local success, err = self:set_specific_schema(t.name)
  if not success then return success, err end
  
  return Plugins.super.insert(self, t)
end

function Plugins:find_distinct()
  -- Open session
  local session, err = Plugins.super._open_session(self)
  if err then
    return nil, err
  end

  local select_q = query_builder.select(self._table)

  -- Execute query
  local distinct_names = {}
  for rows, err in Plugins.super.execute(self, select_q, nil, nil, {auto_paging=true}) do
    if err then
      return nil, err
    end
    for _, v in ipairs(rows) do
      -- Rows also contains other properties, so making sure it's a plugin
      if v.name then
        distinct_names[v.name] = true
      end
    end
  end

  -- Close session
  local socket_err = Plugins.super._close_session(self, session)
  if socket_err then
    return nil, socket_err
  end

  local result = {}
  for k, _ in pairs(distinct_names) do
    table.insert(result, k)
  end

  return result, nil
end

return {plugins = Plugins}
