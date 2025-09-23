DELIMITER //
CREATE OR REPLACE PROCEDURE get_distinct_err_msg_multi_tables(
    IN in_db VARCHAR(64),
    IN in_tables TEXT,
    IN from_time VARCHAR(30), 
    IN to_time VARCHAR(30)
)
BEGIN
DECLARE pos INT DEFAULT 1;
DECLARE tbl VARCHAR(64);
DECLARE comma_pos INT DEFAULT 0;
DECLARE next_comma_pos INT;
DECLARE union_sql TEXT DEFAULT '';

DECLARE tbl_count INT;

SET tbl_count = LENGTH(in_tables) - LENGTH(REPLACE(in_tables, ',', '')) + 1;
SET @from_dt = STR_TO_DATE(SUBSTRING_INDEX(from_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');
SET @to_dt   = STR_TO_DATE(SUBSTRING_INDEX(to_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');

WHILE pos <= tbl_count DO
    IF pos = 1 THEN
    SET next_comma_pos = LOCATE(',', in_tables);
    IF next_comma_pos = 0 THEN
        SET tbl = TRIM(in_tables);
    ELSE
        SET tbl = TRIM(SUBSTRING(in_tables, 1, next_comma_pos - 1));
    END IF;
    ELSE
    SET comma_pos = LOCATE(',', in_tables, comma_pos + 1);
    SET next_comma_pos = LOCATE(',', in_tables, comma_pos + 1);
    IF next_comma_pos = 0 THEN
        SET tbl = TRIM(SUBSTRING(in_tables, comma_pos + 1));
    ELSE
        SET tbl = TRIM(SUBSTRING(in_tables, comma_pos + 1, next_comma_pos - comma_pos - 1));
    END IF;
    END IF;

    SET union_sql = CONCAT(
    union_sql,
    IF(union_sql = '', '', ' UNION '),
    'SELECT DISTINCT err_msg FROM `', in_db, '`.`', tbl, '` WHERE err_msg IS NOT NULL ',
    "AND tbeg BETWEEN '", @from_dt, "' AND '", @to_dt, "'"
    );

    SET pos = pos + 1;
END WHILE;

SET @sql = union_sql;

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
END //