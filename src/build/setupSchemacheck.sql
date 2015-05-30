CREATE USER 'schemacheck'@'localhost'
  IDENTIFIED BY 'schemacheck';
GRANT USAGE
  ON *.*
  TO 'schemacheck'@'localhost' IDENTIFIED BY 'schemacheck'
    WITH MAX_QUERIES_PER_HOUR 0 MAX_CONNECTIONS_PER_HOUR 0 MAX_UPDATES_PER_HOUR 0 MAX_USER_CONNECTIONS 0;
GRANT ALL PRIVILEGES
  ON `schemacheck\_%`.*
  TO 'schemacheck'@'localhost';

CREATE USER 'schemacheck'@'127.0.0.1'
  IDENTIFIED BY 'schemacheck';
GRANT USAGE
  ON *.*
  TO 'schemacheck'@'127.0.0.1' IDENTIFIED BY 'schemacheck'
    WITH MAX_QUERIES_PER_HOUR 0 MAX_CONNECTIONS_PER_HOUR 0 MAX_UPDATES_PER_HOUR 0 MAX_USER_CONNECTIONS 0;
GRANT ALL PRIVILEGES
  ON `schemacheck\_%`.*
  TO 'schemacheck'@'127.0.0.1';
