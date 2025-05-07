

SELECT 'text|varchar|char|\xDEADBEEF|{"key":123}|<xml>data</xml>'::composite;
-- 期望结果（不支持 xml）
--                  composite
-- --------------------------------------------
--  text|varchar|char|\xdeadbeef|{"key": 123}|
-- (1 row)

-- 期望结果（支持 xml）
--                  composite
-- --------------------------------------------
--  text|varchar|char|\xdeadbeef|{"key": 123}|<xml>data</xml>
-- (1 row)