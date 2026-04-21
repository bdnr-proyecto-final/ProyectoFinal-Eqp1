-- Ejemplo de roles para una configuración con autenticación habilitada.
-- Este archivo documenta la estrategia de control de accesos de la etapa 2.
-- No se ejecuta en la configuración actual por defecto del contenedor Cassandra.

-- CREATE ROLE admin WITH PASSWORD = 'admin123' AND LOGIN = true;
-- CREATE ROLE analyst WITH PASSWORD = 'analyst123' AND LOGIN = true;

-- GRANT ALL PERMISSIONS ON KEYSPACE wikimedia TO admin;
-- GRANT SELECT ON KEYSPACE wikimedia TO analyst;