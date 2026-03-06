-- API Sync Strategy Fixed Params
-- Version: 016
-- Description: Add fixed_params and fixed_param_keys columns to api_sync_strategies
--              for configurable default parameters (e.g. fields) and keys that
--              must not be overridden by callers.

ALTER TABLE api_sync_strategies ADD COLUMN fixed_params TEXT;
ALTER TABLE api_sync_strategies ADD COLUMN fixed_param_keys TEXT;

