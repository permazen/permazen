-- $Id$

CREATE TABLE `SchemaUpdate` (
   `updateName` VARCHAR(255) NOT NULL,
   `updateTime` DATETIME NOT NULL,
   PRIMARY KEY(`updateName`)
) ENGINE=InnoDB default charset=utf8 collate=utf8_bin

