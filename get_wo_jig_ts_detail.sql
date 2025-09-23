DELIMITER //
CREATE OR REPLACE PROCEDURE `get_wo_jig_ts_detail`(
    IN in_db VARCHAR(64), 
    IN in_wo TEXT,
    IN in_ts VARCHAR(64),  -- table name
    IN in_jig VARCHAR(64)
)
BEGIN
    proc_end: BEGIN

    DECLARE sql_query TEXT DEFAULT '';
    DECLARE table_sql TEXT DEFAULT '';
    DECLARE temp_expr TEXT;
    DECLARE temp_check_expr TEXT;

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
        "WHERE wo IN (", in_wo, ") AND is_y = 1 AND jig = '", 
        in_jig, "'"
    );

    SET sql_query = table_sql;

    -- Final summary query
    IF sql_query = '' THEN
        SELECT 'No such table found in the specified database.' AS message;
    ELSE
        SET @final_query = CONCAT(
        "SELECT jig, station, ",
        "COUNT(CASE WHEN err_msg IS NULL THEN sn END) AS pass_count, ",
        "COUNT(CASE WHEN err_msg IS NOT NULL THEN sn END) AS fail_count, ",
        "ROUND(100.0 * COUNT(CASE WHEN err_msg IS NOT NULL THEN sn END) / COUNT(*), 2) AS fail_percentage, ",
        "ROUND(AVG(NULLIF(temp_val, -9999)), 2) AS ave_temp, ",
        "ROUND(MAX(NULLIF(temp_val, -9999)), 2) AS max_temp ",
        "FROM (", sql_query, ") combined ",
        "GROUP BY jig, station ",
        "ORDER BY jig, station"
        );

        PREPARE stmt FROM @final_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;

    END proc_end;
END //