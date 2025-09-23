DELIMITER //
CREATE OR REPLACE PROCEDURE `get_jig_temp_last_ten`(
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
    DECLARE jig_list_str TEXT DEFAULT NULL;

    DECLARE cur CURSOR FOR
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = in_db AND table_name = 'pt';

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    -- Step 1: Build union query to get distinct WOs in time range
    SET @jig_union_sql = '';
    SET @from_dt = STR_TO_DATE(SUBSTRING_INDEX(from_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');
    SET @to_dt   = STR_TO_DATE(SUBSTRING_INDEX(to_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');

    OPEN cur;

    read_loop1: LOOP
        FETCH cur INTO tbl_name;
        IF done THEN
        LEAVE read_loop1;
        END IF;

        SET @table_sql = CONCAT(
        "SELECT DISTINCT jig FROM ", in_db, ".", tbl_name,
        " WHERE tbeg BETWEEN '", @from_dt, "' AND '", @to_dt, "'",
        " AND is_y = 1"
        );

        IF @jig_union_sql = '' THEN
        SET @jig_union_sql = @table_sql;
        ELSE
        SET @jig_union_sql = CONCAT(@jig_union_sql, " UNION ", @table_sql);
        END IF;
    END LOOP;

    CLOSE cur;

    -- Step 2: Create temporary table
    DROP TEMPORARY TABLE IF EXISTS tmp_jig_list;
    CREATE TEMPORARY TABLE tmp_jig_list(jig_list TEXT);

    -- Step 3: Build the final SQL and execute
    SET @full_sql = CONCAT(
        "INSERT INTO tmp_jig_list (jig_list) ",
        "SELECT GROUP_CONCAT(DISTINCT QUOTE(jig)) FROM (", @jig_union_sql, ") AS recent_jig"
    );

    PREPARE stmt FROM @full_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    -- Step 3: Select CSV string into variable
    SELECT jig_list INTO jig_list_str FROM tmp_jig_list LIMIT 1;

    -- If no WOs found, return message and exit
    IF jig_list_str IS NULL THEN
        SELECT 'No jig found in the timeframe.' AS message;
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
        "SELECT jig, soc_temp_max, tbeg, '", tbl_name, "' AS station ",
        "FROM ", in_db, ".", tbl_name, " ",
        "WHERE jig IN (", jig_list_str, ") AND is_y = 1"
        );

        IF sql_query = '' THEN
        SET sql_query = table_sql;
        ELSE
        SET sql_query = CONCAT(sql_query, " UNION ALL ", table_sql);
        END IF;
    END LOOP;

    CLOSE cur;

    IF sql_query = '' THEN
        SELECT 'No valid tables with jig data found.' AS message;
    ELSE
        SET @final_query = CONCAT(
        "WITH ordered_logs AS ( ",
            "SELECT jig, soc_temp_max, station, ",
                "ROW_NUMBER() OVER (PARTITION BY jig ORDER BY tbeg DESC) AS rn ",
            "FROM (", sql_query, ") AS combined ",
        "), ",
        "filtered_jigs AS ( ",
            "SELECT jig, COUNT(*) as count_above_75 FROM ordered_logs ",
            "WHERE rn <= 10 AND soc_temp_max > 75 ",
            "GROUP BY jig HAVING COUNT(*) >= 6 ",
        ") ",
        "SELECT f.jig, f.count_above_75 ",
        "FROM filtered_jigs f "
        );

        PREPARE stmt FROM @final_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;
    END proc_end;
END //