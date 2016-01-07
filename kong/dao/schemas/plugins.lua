local utils = require "kong.tools.utils"
local DaoError = require "kong.dao.error"
local constants = require "kong.constants"

return {
  name = "Plugin configuration",
  primary_key = {"id"},
  clustering_key = {"name"},
  fields = {
    id = {
        type = "id",
        dao_insert_value = true },
    created_at = {
        type = "timestamp",
        dao_insert_value = true },
    api_id = {
        type = "id",
        required = true,
        foreign = "apis:id",
        queryable = true },
    consumer_id = {
        type = "id",
        foreign = "consumers:id",
        queryable = true,
        default = constants.DATABASE_NULL_ID },
    name = {
        type = "string",
        required = true,
        immutable = true,
        queryable = true },
    config = {
        type = "table",
        schema = nil,  -- this will be set before writing to the db, specific for each plugin
        default = {} },
    enabled = {
        type = "boolean",
        default = true }
  },
  self_check = function(self, plugin_t, dao, is_update)
    local config_schema = self.fields.config.schema

    -- Check if the schema has a `no_consumer` field
    if config_schema.no_consumer and plugin_t.consumer_id ~= nil and plugin_t.consumer_id ~= constants.DATABASE_NULL_ID then
      return false, DaoError("No consumer can be configured for that plugin", constants.DATABASE_ERROR_TYPES.SCHEMA)
    end

    if type(config_schema.self_check) == "function" then
      local ok, err = config_schema.self_check(config_schema, plugin_t.config and plugin_t.config or {}, dao, is_update)
      if not ok then
        return false, err
      end
    end

    if not is_update then
      local res, err = dao.plugins:find_by_keys({
        name = plugin_t.name,
        api_id = plugin_t.api_id,
        consumer_id = plugin_t.consumer_id
      })

      if err then
        return nil, DaoError(err, constants.DATABASE_ERROR_TYPES.DATABASE)
      end

      if res and #res > 0 then
        return false, DaoError("Plugin configuration already exists", constants.DATABASE_ERROR_TYPES.UNIQUE)
      end
    end
  end
}
