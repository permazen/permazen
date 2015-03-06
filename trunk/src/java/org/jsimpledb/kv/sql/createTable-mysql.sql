-- $Id$

CREATE TABLE IF NOT EXISTS `KV` (
    -- MySQL InnoDB indexes are limited to 767 bytes
    -- You can go up to 3072 bytes if you set innodb_large_prefix 
    -- See: http://dev.mysql.com/doc/refman/5.5/en/innodb-restrictions.html
    -- Also recommended: innodb_file_per_table
   `kv_key` VARBINARY(767) NOT NULL,
   `kv_value` LONGBLOB NOT NULL,
   PRIMARY KEY(`kv_key`)
) ENGINE=InnoDB default charset=utf8 collate=utf8_bin
