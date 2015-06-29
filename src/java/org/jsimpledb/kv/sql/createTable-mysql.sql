CREATE TABLE IF NOT EXISTS `KV` (
   `kv_key` VARBINARY(767) NOT NULL,
   `kv_value` LONGBLOB NOT NULL,
   PRIMARY KEY(`kv_key`)
) ENGINE=InnoDB default charset=utf8 collate=utf8_bin

--
-- MySQL InnoDB indexes are normally limited to 767 bytes, but
-- you can go up to 3072 bytes if you set innodb_large_prefix=true;
-- see http://dev.mysql.com/doc/refman/5.5/en/innodb-restrictions.html
--
-- For example:
--
--  CREATE TABLE IF NOT EXISTS `KV` (
--      `kv_key` VARBINARY(3072) NOT NULL,
--      `kv_value` LONGBLOB NOT NULL,
--      PRIMARY KEY(`kv_key`)
--  ) ENGINE=InnoDB DEFAULT charset=utf8 collate=utf8_bin ROW_FORMAT=DYNAMIC;
--
-- And in /etc/my.cnf:
--
--  [mysqld]
--  innodb_large_prefix      = true
--  innodb_file_per_table    = true
--  innodb_file_format       = barracuda
--
