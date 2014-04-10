-- $Id$

CREATE TABLE IF NOT EXISTS `KV` (
   `kv_key` VARBINARY(767) NOT NULL,
   `kv_value` VARBINARY(767) NOT NULL,
   PRIMARY KEY(`kv_key`)
) ENGINE=InnoDB default charset=utf8 collate=utf8_bin
