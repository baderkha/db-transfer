 SELECT 			
  					table_schema as db_name,
  					table_name as tb_name,
  					column_name AS column_name,
                    data_type AS data_type,
                    column_type AS column_type,
                    safe_sql_value AS safe_sql_value
                FROM (SELECT
                			table_schema,
                			table_name, 
                			column_name,
                            upper(data_type) as data_type,
                            column_type,
                            CASE
                            WHEN data_type IN ('blob', 'tinyblob', 'mediumblob', 'longblob')
                                    THEN CONCAT('REPLACE(hex(`', column_name, '`)', ", '
', ' ')")
                            WHEN data_type IN ('binary', 'varbinary')
                                    THEN concat('REPLACE(REPLACE(hex(trim(trailing CHAR(0x00) from `',COLUMN_NAME,'`))', ", '
', ' '), '
', '')")
                            WHEN data_type IN ('bit')
                                    THEN concat('cast(`', column_name, '` AS unsigned)')
                            WHEN data_type IN ('date')
                                    THEN concat('nullif(CAST(`', column_name, '` AS date),STR_TO_DATE("0000-00-00 00:00:00", "%Y-%m-%d %T"))')
                            WHEN data_type IN ('datetime', 'timestamp')
                                    THEN concat('nullif(`', column_name, '`,STR_TO_DATE("0000-00-00 00:00:00", "%Y-%m-%d %T"))')
                            WHEN column_type IN ('tinyint(1)')
                                    THEN concat('CASE WHEN `' , column_name , '` is null THEN null WHEN `' , column_name , '` = 0 THEN 0 ELSE 1 END')
                            WHEN column_type IN ('geometry', 'point', 'linestring', 'polygon', 'multipoint', 'multilinestring', 'multipolygon', 'geometrycollection')
                                    THEN concat('ST_AsGeoJSON(', column_name, ')')
                            WHEN column_name = 'raw_data_hash'
                                    THEN concat('REPLACE(REPLACE(hex(`', column_name, '`)', ", '
', ' '), '
', '')")
                            WHEN data_type IN ('double', 'numeric', 'float', 'decimal', 'real')
                                    THEN 
              CONCAT('`', column_name, '`')
            
                            WHEN data_type IN ('smallint', 'integer', 'bigint', 'mediumint', 'int')
                                    THEN 
              CONCAT('`', column_name, '`')
            
                            ELSE concat('REPLACE(REPLACE(REPLACE(cast(`', column_name, '` AS char CHARACTER SET utf8)', ", '
', ' '), '
', ''), '\0', '')")
                                END AS safe_sql_value,
                            ordinal_position
                    FROM information_schema.columns
                    WHERE  table_schema not in ('information_schema','mysql','performance_schema','sys') ) as x
                    order by db_name,table_name
                