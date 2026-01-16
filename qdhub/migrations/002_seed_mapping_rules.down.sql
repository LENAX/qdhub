-- Rollback: Remove seeded default data type mapping rules
-- Version: 002

DELETE FROM data_type_mapping_rules WHERE is_default = 1;
