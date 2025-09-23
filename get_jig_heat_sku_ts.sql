DELIMITER //
CREATE OR REPLACE PROCEDURE `get_jig_heat_sku_ts`(
    IN in_db VARCHAR(64), 
    IN in_ts VARCHAR(64),
    IN in_jig VARCHAR(64),
    IN from_time VARCHAR(30), 
    IN to_time VARCHAR(30)
)
BEGIN
    proc_end: BEGIN

    DECLARE sql_query TEXT DEFAULT '';
    DECLARE table_sql TEXT DEFAULT '';
    DECLARE temp_expr TEXT;
    DECLARE temp_check_expr TEXT;

    SET @from_dt = STR_TO_DATE(SUBSTRING_INDEX(from_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');
    SET @to_dt   = STR_TO_DATE(SUBSTRING_INDEX(to_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');

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

    -- Build CASE + GREATEST expression safely
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
        "AND is_y = 1 AND jig = '", in_jig, "'"
    );

    SET sql_query = table_sql;

    -- Final summary query
    IF sql_query = '' THEN
        SELECT 'No such table found in the specified database.' AS message;
    ELSE
        SET @final_query = CONCAT(
        "SELECT jig, ",
        "COUNT(*) AS number_of_tests, ",
        "COUNT(CASE WHEN err_msg IS NULL THEN sn END) AS pass_count, ",
        "COUNT(CASE WHEN err_msg IS NOT NULL THEN sn END) AS fail_count, ",
        "ROUND(100.0 * COUNT(CASE WHEN err_msg IS NOT NULL THEN sn END) / COUNT(*), 2) AS fail_percentage, ",
        "ROUND(AVG(NULLIF(temp_val, -9999)), 2) AS avg_temp, ",
        "ROUND(MAX(NULLIF(temp_val, -9999)), 2) AS max_temp ",
        "FROM (", sql_query, ") combined "
        );

        PREPARE stmt FROM @final_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;

    END proc_end;
END //