DELIMITER //
CREATE OR REPLACE PROCEDURE get_all_jig_suffix(
    IN in_db VARCHAR(64),
    IN from_time VARCHAR(30), 
    IN to_time VARCHAR(30)
)
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE tablename VARCHAR(64);
    DECLARE sql_query TEXT DEFAULT '';
    DECLARE cur CURSOR FOR
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE COLUMN_NAME = 'jig' AND TABLE_SCHEMA = in_db;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    SET @from_dt = STR_TO_DATE(SUBSTRING_INDEX(from_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');
    SET @to_dt = STR_TO_DATE(SUBSTRING_INDEX(to_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');

    OPEN cur;

    read_loop: LOOP
        FETCH cur INTO tablename;
        IF done THEN
        LEAVE read_loop;
        END IF;

        IF sql_query = '' THEN
        SET sql_query = CONCAT(
            'SELECT SUBSTRING_INDEX(jig, ''-'', -1) AS jig_suffix FROM `', in_db, '`.`', tablename, '` WHERE jig IS NOT NULL ', 
            "AND tbeg BETWEEN '", @from_dt, "' AND '", @to_dt, "'"
        );
        ELSE
        SET sql_query = CONCAT(
            sql_query, ' UNION ALL ',
            'SELECT SUBSTRING_INDEX(jig, ''-'', -1) AS jig_suffix FROM `', in_db, '`.`', tablename, '` WHERE jig IS NOT NULL ',
            "AND tbeg BETWEEN '", @from_dt, "' AND '", @to_dt, "'"
        );
        END IF;
    END LOOP;

    CLOSE cur;

    IF sql_query != '' THEN
        SET @final_sql = CONCAT(
        'SELECT DISTINCT jig_suffix FROM (', sql_query, ') AS combined ORDER BY jig_suffix'
        );

        PREPARE stmt FROM @final_sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    ELSE
        SELECT 'No tables with a jig column found.' AS message;
    END IF;
END//
