DELIMITER //
CREATE OR REPLACE PROCEDURE `get_rpi_yield_summary_wo`(
    IN in_db VARCHAR(64), 
    IN in_wo TEXT,
    IN in_ts TEXT
)
BEGIN
    proc_end: BEGIN

    DECLARE sql_query TEXT DEFAULT '';
    DECLARE table_sql TEXT DEFAULT '';
    DECLARE tbl_name VARCHAR(64);
    DECLARE done INT DEFAULT FALSE;

    DECLARE cur CURSOR FOR SELECT table_name FROM temp_table_list;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    IF TRIM(in_wo) = '' THEN
        SELECT 'No wo selected' AS message;
        LEAVE proc_end;
    END IF;

    CREATE TEMPORARY TABLE IF NOT EXISTS temp_table_list (table_name VARCHAR(64));
    DELETE FROM temp_table_list;

    SET @table_list_sql = CONCAT(
    'INSERT INTO temp_table_list ',
    'SELECT table_name FROM information_schema.tables ',
    'WHERE table_schema = ? AND table_name IN (', in_ts, ')'
    );
    PREPARE stmt FROM @table_list_sql;
    EXECUTE stmt USING in_db;
    DEALLOCATE PREPARE stmt;

    SET done = FALSE;
    OPEN cur;

    read_loop: LOOP
        FETCH cur INTO tbl_name;
        IF done THEN
        LEAVE read_loop;
        END IF;

        -- Build SQL for this table
        SET table_sql = CONCAT(
        "SELECT wo, sn, err_msg, tbeg, jig, '", tbl_name, "' AS station, rpi_temp_max ",
        "FROM ", in_db, ".", tbl_name, " ",
        "WHERE wo IN (", in_wo, ") AND is_y = 1 AND jig IS NOT NULL"
        );

        -- Accumulate the SQL
        IF sql_query = '' THEN
        SET sql_query = table_sql;
        ELSE
        SET sql_query = CONCAT(sql_query, " UNION ALL ", table_sql);
        END IF;
    END LOOP;

    CLOSE cur;
    DROP TEMPORARY TABLE IF EXISTS temp_table_list;

    -- Final summary query
    IF sql_query = '' THEN
        SELECT 'No relevant tables found in the specified database.' AS message;
    ELSE
        SET @final_query = CONCAT(
        "SELECT jig, station, ",
        "COUNT(CASE WHEN err_msg IS NULL THEN sn END) AS pass_count, ",
        "COUNT(CASE WHEN err_msg IS NOT NULL THEN sn END) AS fail_count, ",
        "ROUND(100.0 * COUNT(CASE WHEN err_msg IS NOT NULL THEN sn END) / COUNT(*), 2) AS fail_percentage, ",
        "AVG(rpi_temp_max) AS avg_rpi_temp, ",
        "MAX(rpi_temp_max) AS max_rpi_temp ",
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