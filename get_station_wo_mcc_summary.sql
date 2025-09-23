DELIMITER //
CREATE OR REPLACE PROCEDURE `get_station_wo_mcc_summary`(
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
    DECLARE wo_list_str TEXT DEFAULT NULL;
    DECLARE check_mcc_upload INT DEFAULT 0;
    DECLARE check_mcc_check INT DEFAULT 0;
    DECLARE columns TEXT DEFAULT '';

    DECLARE cur CURSOR FOR
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = in_db AND table_name IN ('pt'); -- ADD WHEN NEEDED

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    -- Step 1: Build union query to get distinct WOs in time range
    SET @wo_union_sql = '';
    SET @from_dt = STR_TO_DATE(SUBSTRING_INDEX(from_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');
    SET @to_dt = STR_TO_DATE(SUBSTRING_INDEX(to_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');

    OPEN cur;

    read_loop1: LOOP
        FETCH cur INTO tbl_name;
        IF done THEN
            LEAVE read_loop1;
        END IF;

        SET @table_sql = CONCAT(
            "SELECT wo FROM ", in_db, ".", tbl_name,
            " WHERE wo IN (",
                "SELECT DISTINCT wo FROM ", in_db, ".", tbl_name,
                " WHERE tbeg BETWEEN '", @from_dt, "' AND '", @to_dt, "'",
                " AND is_y = 1",
            ") ",
            "GROUP BY wo "
        );

        IF @wo_union_sql = '' THEN
            SET @wo_union_sql = @table_sql;
        ELSE
            SET @wo_union_sql = CONCAT(@wo_union_sql, " UNION ", @table_sql);
        END IF;
    END LOOP;

    CLOSE cur;

    -- Step 2: Create temporary table
    DROP TEMPORARY TABLE IF EXISTS tmp_wo_list;
    CREATE TEMPORARY TABLE tmp_wo_list(wo_list TEXT);

    -- Step 3: Build the final SQL and execute
    SET @full_sql = CONCAT(
        "INSERT INTO tmp_wo_list (wo_list) ",
        "SELECT GROUP_CONCAT(DISTINCT QUOTE(wo)) FROM (", @wo_union_sql, ") AS recent_wos"
    );

    PREPARE stmt FROM @full_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    -- Step 3: Select CSV string into variable
    SELECT wo_list INTO wo_list_str FROM tmp_wo_list LIMIT 1;

    -- If no WOs found, return message and exit
    IF wo_list_str IS NULL THEN
        SELECT 'No work orders found in the timeframe.' AS message;
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

        -- Reset flags
        SET check_mcc_upload = 0;
        SET check_mcc_check = 0;

        -- Check if columns exist
        SELECT COUNT(*) INTO check_mcc_upload FROM information_schema.columns 
        WHERE table_schema = in_db AND table_name = tbl_name AND column_name = 'mcc_upload';

        SELECT COUNT(*) INTO check_mcc_check FROM information_schema.columns 
        WHERE table_schema = in_db AND table_name = tbl_name AND column_name = 'mcc_check';

        -- Start building SELECT clause
        SET columns = "wo, sn, err_msg, tbeg";

        -- Conditionally add real or NULL columns to keep column count consistent
        IF check_mcc_upload > 0 THEN
            SET columns = CONCAT(columns, ", mcc_upload");
        ELSE
            SET columns = CONCAT(columns, ", NULL AS mcc_upload");
        END IF;

        IF check_mcc_check > 0 THEN
            SET columns = CONCAT(columns, ", mcc_check");
        ELSE
            SET columns = CONCAT(columns, ", NULL AS mcc_check");
        END IF;

        SET table_sql = CONCAT(
            "SELECT ", columns, ", '", tbl_name, "' AS station ", " FROM ", in_db, ".", tbl_name, " ",
            "WHERE wo IN (", wo_list_str, ") AND is_y = 1"
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
        SELECT 'No pt/pts/pdlp tables found in the specified database.' AS message;
    ELSE
        SET @final_query = CONCAT(
            "SELECT wo, station, MIN(tbeg) AS min_tbeg, ",
            "ROUND(100.0 * COUNT(CASE WHEN mcc_check = 'PASS' THEN 1 END) / NULLIF(COUNT(CASE WHEN mcc_check IN ('PASS', 'FAIL') THEN 1 END), 0), 3) AS mcc_check_rate,",
            "ROUND(100.0 * COUNT(CASE WHEN mcc_upload = 'PASS' THEN 1 END) / NULLIF(COUNT(CASE WHEN mcc_upload IN ('PASS', 'FAIL') THEN 1 END), 0), 3) AS mcc_upload_rate ",
            "FROM (", sql_query, ") combined ",
            "GROUP BY wo, station "
        );

        PREPARE stmt FROM @final_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;

    END proc_end;
END //
DELIMITER ;
