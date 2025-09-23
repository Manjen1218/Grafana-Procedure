DELIMITER //
CREATE OR REPLACE PROCEDURE `get_riser_sn_yield_summary_threshold`(
    IN in_db VARCHAR(64), 
    IN from_time VARCHAR(30), 
    IN to_time VARCHAR(30)
)
BEGIN
    proc_end: BEGIN

    DECLARE sql_query TEXT DEFAULT '';
    DECLARE table_sql TEXT DEFAULT '';
    DECLARE tbl_name VARCHAR(64);
    DECLARE done INT DEFAULT FALSE;
    DECLARE riser_sn_list_str TEXT DEFAULT NULL;
    DECLARE table_count INT DEFAULT 0;

    DECLARE cur CURSOR FOR
    SELECT table_name
    FROM information_schema.columns
    WHERE table_schema = in_db
        AND column_name = 'riser_sn';
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    SELECT COUNT(*) INTO table_count
    FROM information_schema.columns
    WHERE table_schema = in_db
        AND column_name = 'riser_sn';

    IF table_count = 0 THEN
        SELECT 'No table with riser_sn' AS MESSAGE;
        LEAVE proc_end;
    END IF;

    -- Step 1: Build union query to get distinct WOs in time range
    SET @riser_sn_union_sql = '';
    SET @from_dt = STR_TO_DATE(SUBSTRING_INDEX(from_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');
    SET @to_dt   = STR_TO_DATE(SUBSTRING_INDEX(to_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');

    OPEN cur;

    read_loop1: LOOP
        FETCH cur INTO tbl_name;
        IF done THEN
        LEAVE read_loop1;
        END IF;

        SET @table_sql = CONCAT(
        "SELECT DISTINCT riser_sn FROM ", in_db, ".", tbl_name,
        " WHERE tbeg BETWEEN '", @from_dt, "' AND '", @to_dt, "'",
        " AND is_y = 1"
        );

        IF @riser_sn_union_sql = '' THEN
        SET @riser_sn_union_sql = @table_sql;
        ELSE
        SET @riser_sn_union_sql = CONCAT(@riser_sn_union_sql, " UNION ", @table_sql);
        END IF;
    END LOOP;

    CLOSE cur;

    -- Step 2: Create temporary table
    DROP TEMPORARY TABLE IF EXISTS tmp_riser_sn_list;
    CREATE TEMPORARY TABLE tmp_riser_sn_list(riser_sn_list TEXT);

    -- Step 3: Build the final SQL and execute
    SET @full_sql = CONCAT(
        "INSERT INTO tmp_riser_sn_list (riser_sn_list) ",
        "SELECT GROUP_CONCAT(DISTINCT QUOTE(riser_sn)) FROM (", @riser_sn_union_sql, ") AS recent_riser_sns"
    );

    PREPARE stmt FROM @full_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    -- Step 3: Select CSV string into variable
    SELECT riser_sn_list INTO riser_sn_list_str FROM tmp_riser_sn_list LIMIT 1;

    -- If no WOs found, return message and exit
    IF riser_sn_list_str IS NULL THEN
        SELECT 'No riser_sn found in the timeframe.' AS MESSAGE;
        LEAVE proc_end;
    END IF;

    -- Step 4: Build main query to get full data for those WOs
    SET done = FALSE;
    SET sql_query = '';
    OPEN cur;

    read_loop2: LOOP
        FETCH cur INTO tbl_name;
        IF done THEN
        LEAVE read_loop2;
        END IF;

        SET table_sql = CONCAT(
        "SELECT riser_sn, '", tbl_name, "' AS station, ",
        "COUNT(CASE WHEN err_msg IS NULL THEN sn END) AS pass_count, ",
        "COUNT(CASE WHEN err_msg IS NOT NULL THEN sn END) AS fail_count, ",
        "COUNT(*) AS total, "
        "ROUND(100 * COUNT(CASE WHEN err_msg IS NULL THEN sn END) / COUNT(*), 2) AS yield "
        "FROM ", in_db, ".", tbl_name, " ",
        "WHERE riser_sn IN (", riser_sn_list_str, ") AND is_y = 1 ",
        "GROUP BY riser_sn "
        );

        IF sql_query = '' THEN
        SET sql_query = table_sql;
        ELSE
        SET sql_query = CONCAT(sql_query, " UNION ALL ", table_sql);
        END IF;
    END LOOP;

    CLOSE cur;

    -- Step 5: Final aggregation and filtering
    IF sql_query = '' THEN
        SELECT 'No tables with riser_sn found in the specified database.' AS message;
    ELSE
        SET @final_query = CONCAT(sql_query, "ORDER BY yield ");

        PREPARE stmt FROM @final_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;

    END proc_end;
END //