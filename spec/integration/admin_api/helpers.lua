local json = require "cjson"
local http_client = require "kong.tools.http_client"
local spec_helper = require "spec.spec_helpers"
local assert = require "luassert"

--- http request helper function for testing the admin api.
-- Performs an http request based on `url`, `method` and `body`, and compares the result
-- agansit the expected results `res_status` and `res_body`. The request is made twice,
-- the second time explicitly with "application/json" set as content-type.
-- @param url The request url
-- @param method the http method to use for the request
-- @param body the body to use for the request
-- @param res_status the expected status code returned (errors if the actual code of either request differs)
-- @param res_body (optional) if given, it must equal both responses, or it will error.
-- @param options (optional) An options table supporting option `options.drop_db = true` to drop in between both requests
local function send_content_types(url, method, body, res_status, res_body, options)
  if not options then options = {} end

  local form_response, form_status = http_client[method:lower()](url, body)
  assert.equal(res_status, form_status)

  if options.drop_db then
    spec_helper.drop_db()
  end

  local json_response, json_status = http_client[method:lower()](url, body, {["content-type"]="application/json"})
  assert.equal(res_status, json_status)

  if res_body then
    assert.same(res_body.."\n", form_response)
    assert.same(res_body.."\n", json_response)
  end

  local res_obj
  local status, res = pcall(function() res_obj = json.decode(json_response) end)
  if not status then
    error(res, 2)
  end

  return res_obj
end

return send_content_types
