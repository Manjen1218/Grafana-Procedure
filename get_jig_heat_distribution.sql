DELIMITER //
CREATE OR REPLACE PROCEDURE `get_jig_heat_distribution`(
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
    DECLARE temp_expr TEXT;
    DECLARE temp_check_expr TEXT;

    DECLARE prefix TEXT;
    DECLARE prefix_pos INT DEFAULT 1;
    DECLARE prefix_count INT;
    DECLARE prefix_clause TEXT DEFAULT '';
    
    IF in_jig IS NULL OR LENGTH(TRIM(in_jig)) = 0 THEN
        SELECT 'No JIG prefixes provided.' AS message;
        LEAVE proc_end;
    END IF;

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

    -- Detect available temperature columns in the specified table only
    SELECT 
        GROUP_CONCAT(CONCAT('IFNULL(', column_name, ', -9999)') ORDER BY column_name SEPARATOR ', '),
        GROUP_CONCAT(CONCAT(column_name, ' IS NOT NULL') ORDER BY column_name SEPARATOR ' OR ')
    INTO 
        temp_expr,
        temp_check_expr
    FROM information_schema.columns
    WHERE table_schema = in_db
        AND table_name = in_ts
        AND column_name IN (
            'soc_temp_max', 'critical_temp_max', 'berg5_soc_t_max', 'hpl_soc_temp_max', 'mcf_soc_t_max'
        );

    IF temp_expr IS NOT NULL AND temp_expr != '' AND temp_check_expr IS NOT NULL AND temp_check_expr != '' THEN
        SET temp_expr = CONCAT(
            "CASE WHEN (", temp_check_expr, ") ",
            "THEN GREATEST(", temp_expr, ") ELSE -9999 END AS temp_val"
        );
    ELSE
        SET temp_expr = "-9999 AS temp_val";
    END IF;

    -- Build SQL for the specified table only
    SET table_sql = CONCAT(
        "SELECT wo, sn, err_msg, tbeg, jig, '", in_ts, "' AS station, ",
        temp_expr, " ",
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
        "ROUND(AVG(NULLIF(temp_val, -9999)), 2) AS avg_temp, ",
        "ROUND(MAX(NULLIF(temp_val, -9999)), 2) AS max_temp ",
        "FROM (", sql_query, ") combined ",
        "GROUP BY jig ",
        "ORDER BY avg_temp"
    );

    PREPARE stmt FROM @final_query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    END proc_end;
END //

DELIMITER ;
