DELIMITER //
CREATE OR REPLACE PROCEDURE `get_gbic_sn_yield_summary_threshold`(
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
    DECLARE gbic_sn_list_str TEXT DEFAULT NULL;
    DECLARE table_count INT DEFAULT 0;

    DECLARE cur CURSOR FOR
    SELECT table_name
    FROM information_schema.columns
    WHERE table_schema = in_db
        AND column_name = 'gbic_sn';
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    SELECT COUNT(*) INTO table_count
    FROM information_schema.columns
    WHERE table_schema = in_db
        AND column_name = 'gbic_sn';

    IF table_count = 0 THEN
        SELECT 'No table with gbic_sn' AS MESSAGE;
        LEAVE proc_end;
    END IF;

    -- Step 1: Build union query to get distinct WOs in time range
    SET @gbic_sn_union_sql = '';
    SET @from_dt = STR_TO_DATE(SUBSTRING_INDEX(from_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');
    SET @to_dt   = STR_TO_DATE(SUBSTRING_INDEX(to_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');

    OPEN cur;

    read_loop1: LOOP
        FETCH cur INTO tbl_name;
        IF done THEN
        LEAVE read_loop1;
        END IF;

        SET @table_sql = CONCAT(
        "SELECT DISTINCT gbic_sn FROM ", in_db, ".", tbl_name,
        " WHERE tbeg BETWEEN '", @from_dt, "' AND '", @to_dt, "'",
        " AND is_y = 1"
        );

        IF @gbic_sn_union_sql = '' THEN
        SET @gbic_sn_union_sql = @table_sql;
        ELSE
        SET @gbic_sn_union_sql = CONCAT(@gbic_sn_union_sql, " UNION ", @table_sql);
        END IF;
    END LOOP;

    CLOSE cur;

    -- Step 2: Create temporary table
    DROP TEMPORARY TABLE IF EXISTS tmp_gbic_sn_list;
    CREATE TEMPORARY TABLE tmp_gbic_sn_list(gbic_sn_list TEXT);

    -- Step 3: Build the final SQL and execute
    SET @full_sql = CONCAT(
        "INSERT INTO tmp_gbic_sn_list (gbic_sn_list) ",
        "SELECT GROUP_CONCAT(DISTINCT QUOTE(gbic_sn)) FROM (", @gbic_sn_union_sql, ") AS recent_gbic_sns"
    );

    PREPARE stmt FROM @full_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    -- Step 3: Select CSV string into variable
    SELECT gbic_sn_list INTO gbic_sn_list_str FROM tmp_gbic_sn_list LIMIT 1;

    -- If no WOs found, return message and exit
    IF gbic_sn_list_str IS NULL THEN
        SELECT 'No gbic_sn found in the timeframe.' AS MESSAGE;
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
        "SELECT gbic_sn, '", tbl_name, "' AS station, ",
        "COUNT(CASE WHEN err_msg IS NULL THEN sn END) AS pass_count, ",
        "COUNT(CASE WHEN err_msg IS NOT NULL THEN sn END) AS fail_count, ",
        "COUNT(*) AS total, "
        "ROUND(100 * COUNT(CASE WHEN err_msg IS NULL THEN sn END) / COUNT(*), 2) AS yield "
        "FROM ", in_db, ".", tbl_name, " ",
        "WHERE gbic_sn IN (", gbic_sn_list_str, ") AND is_y = 1 ",
        "GROUP BY gbic_sn "
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
        SELECT 'No tables with gbic_sn found in the specified database.' AS message;
    ELSE
        SET @final_query = CONCAT(sql_query, "ORDER BY yield ");

        PREPARE stmt FROM @final_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;

    END proc_end;
END //