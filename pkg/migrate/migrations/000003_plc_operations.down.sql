-- Drop PLC operations tables in reverse order of creation

DROP VIEW IF EXISTS plc_did_state_mv;
DROP TABLE IF EXISTS plc_did_state;
DROP TABLE IF EXISTS plc_operations;
