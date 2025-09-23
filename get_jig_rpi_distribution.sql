DELIMITER //

CREATE OR REPLACE PROCEDURE `get_jig_rpi_distribution`(
    IN in_db VARCHAR(64), 
    IN in_ts VARCHAR(64),
    IN in_jig TEXT,
    IN from_time VARCHAR(30), 
    IN to_time VARCHAR(30)
)
BEGIN
    proc_end: BEGIN

    DECLARE sql_query TEXT DEFAULT '';
    DECLARE table_sql TEXT DEFAULT '';
    DECLARE prefix_clause TEXT DEFAULT '';
    DECLARE prefix TEXT;
    DECLARE prefix_pos INT DEFAULT 1;
    DECLARE prefix_count INT;

    -- Validate input
    IF in_jig IS NULL OR LENGTH(TRIM(in_jig)) = 0 THEN
        SELECT 'No JIG prefix provided' AS message;
        LEAVE proc_end;
    END IF;

    -- Convert timestamps
    SET @from_dt = STR_TO_DATE(SUBSTRING_INDEX(from_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');
    SET @to_dt = STR_TO_DATE(SUBSTRING_INDEX(to_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');

    -- Count number of prefixes
    SET prefix_count = 1 + LENGTH(in_jig) - LENGTH(REPLACE(in_jig, ',', ''));

    -- Build OR clause: (jig LIKE 'ABC-%' OR jig LIKE 'DEF-%' ...)
    WHILE prefix_pos <= prefix_count + 1 DO
        SET prefix = TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(in_jig, ',', prefix_pos), ',', -1));
        IF prefix IS NOT NULL AND prefix != '' THEN
            IF prefix_clause = '' THEN
                SET prefix_clause = CONCAT("jig LIKE '", prefix, "-%'");
            ELSE
                SET prefix_clause = CONCAT(prefix_clause, " OR jig LIKE '", prefix, "-%'");
            END IF;
        END IF;
        SET prefix_pos = prefix_pos + 1;
    END WHILE;

    IF prefix_clause = '' THEN
        SELECT 'No valid jig prefixes found.' AS message;
        LEAVE proc_end;
    END IF;
    
    -- Build the main query
    SET table_sql = CONCAT(
        "SELECT wo, sn, err_msg, tbeg, jig, rpi_temp_max, '", in_ts, "' AS station ",
        "FROM ", in_db, ".", in_ts, " ",
        "WHERE tbeg BETWEEN '", @from_dt, "' AND '", @to_dt, "' ",
        "AND (", prefix_clause, ") ",
        "AND is_y = 1"
    );

    SET sql_query = table_sql;

    -- Final summary query
    SET @final_query = CONCAT(
        "SELECT jig, ",
        "COUNT(*) AS number_of_tests, ",
        "ROUND(AVG(rpi_temp_max), 2) AS avg_rpi_temp, ",
        "ROUND(MAX(rpi_temp_max), 2) AS max_rpi_temp ",
        "FROM (", sql_query, ") combined ",
        "GROUP BY jig ",
        "ORDER BY max_rpi_temp"
    );

    PREPARE stmt FROM @final_query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    END proc_end;
END //

DELIMITER ;
