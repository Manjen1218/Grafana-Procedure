DELIMITER //
CREATE OR REPLACE PROCEDURE `get_station_yield_summary_wo`(
    IN in_db VARCHAR(64), 
    IN in_wo TEXT
)
BEGIN
    proc_end: BEGIN

    DECLARE sql_query TEXT DEFAULT '';
    DECLARE table_sql TEXT DEFAULT '';
    DECLARE tbl_name VARCHAR(64);
    DECLARE done INT DEFAULT FALSE;
    DECLARE check_mcc_upload INT DEFAULT 0;
    DECLARE check_mcc_check INT DEFAULT 0;
    DECLARE columns TEXT DEFAULT '';

    DECLARE cur CURSOR FOR
        SELECT table_name 
        FROM information_schema.tables
        WHERE table_schema = in_db AND table_name IN ('baking_i', 'baking_o', 'pt', 'pts', 'pdlp');

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    SET done = FALSE;
    OPEN cur;

    read_loop: LOOP
        FETCH cur INTO tbl_name;
        IF done THEN
            LEAVE read_loop;
        END IF;

        -- Check for column existence
        SET check_mcc_upload = 0;
        SET check_mcc_check = 0;

        SELECT COUNT(*) INTO check_mcc_upload 
        FROM information_schema.columns 
        WHERE table_schema = in_db AND table_name = tbl_name AND column_name = 'mcc_upload';

        SELECT COUNT(*) INTO check_mcc_check 
        FROM information_schema.columns 
        WHERE table_schema = in_db AND table_name = tbl_name AND column_name = 'mcc_check';

        -- Base required columns
        SET columns = "wo, sn, err_msg, tbeg, tpver, line";

        -- Conditionally add real or NULL columns to keep all SELECTs consistent
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

        -- Add calculated fields
        SET columns = CONCAT(columns,
            ", '", tbl_name, "' AS station, ",
            "ROW_NUMBER() OVER (PARTITION BY wo, sn ORDER BY tbeg ASC) AS first_rn, ",
            "ROW_NUMBER() OVER (PARTITION BY wo, sn ORDER BY tbeg DESC) AS latest_rn"
        );

        SET table_sql = CONCAT(
            "SELECT ", columns, " FROM ", in_db, ".", tbl_name, " ",
            "WHERE wo IN (", in_wo, ") AND is_y = 1"
        );

        IF sql_query = '' THEN
            SET sql_query = table_sql;
        ELSE
            SET sql_query = CONCAT(sql_query, " UNION ALL ", table_sql);
        END IF;
    END LOOP;

    CLOSE cur;

    IF sql_query = '' THEN
        SELECT 'No relevant tables found in the specified database.' AS message;
    ELSE
        SET @final_query = CONCAT(
            "SELECT station, MIN(tbeg) AS min_tbeg, ",
            "ROUND(100.0 * COUNT(CASE WHEN mcc_check = 'PASS' THEN 1 END) / NULLIF(COUNT(CASE WHEN mcc_check IN ('PASS', 'FAIL') THEN 1 END), 0), 2) AS mcc_check_rate, ",
            "ROUND(100.0 * COUNT(CASE WHEN mcc_upload = 'PASS' THEN 1 END) / NULLIF(COUNT(CASE WHEN mcc_upload IN ('PASS', 'FAIL') THEN 1 END), 0), 2) AS mcc_upload_rate, ",
            "GROUP_CONCAT(DISTINCT tpver ORDER BY tpver SEPARATOR ', ') AS tpver, ",
            "GROUP_CONCAT(DISTINCT line ORDER BY line SEPARATOR ', ') AS line, ",
            "COUNT(DISTINCT CASE WHEN latest_rn = 1 AND err_msg IS NULL THEN sn END) AS pass_count, ",
            "COUNT(DISTINCT CASE WHEN latest_rn = 1 AND err_msg IS NOT NULL THEN sn END) AS fail_count, ",
            "COUNT(DISTINCT sn) AS total_sn, ",
            "ROUND(100.0 * COUNT(DISTINCT CASE WHEN first_rn = 1 AND err_msg IS NULL THEN sn END) / COUNT(DISTINCT sn), 2) AS FPY, ",
            "ROUND(100.0 * COUNT(DISTINCT CASE WHEN latest_rn = 1 AND err_msg IS NULL THEN sn END) / COUNT(DISTINCT sn), 2) AS final_yield_rate ",
            "FROM (", sql_query, ") combined ",
            "GROUP BY station ",
            "ORDER BY station"
        );

        PREPARE stmt FROM @final_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;

    END proc_end;
END //
DELIMITER ;
