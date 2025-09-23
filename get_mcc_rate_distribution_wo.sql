DELIMITER //

CREATE OR REPLACE PROCEDURE get_mcc_check_rate_distribution_wo(
    IN in_schema VARCHAR(64),
    IN in_table VARCHAR(64),
    IN in_wo_filter TEXT
)
BEGIN
    DECLARE column_exists INT;

    -- Step 1: Check if the WO filter is empty or null
    IF in_wo_filter IS NULL OR LENGTH(TRIM(in_wo_filter)) = 0 THEN
        SELECT 'No wo selected' AS message;
    ELSE
        -- Step 2: Check if mcc_err_check column exists
        SELECT COUNT(*) INTO column_exists
        FROM information_schema.COLUMNS 
        WHERE TABLE_SCHEMA = in_schema 
        AND TABLE_NAME = in_table 
        AND COLUMN_NAME = 'mcc_err_check';

        -- Step 3: If column doesn't exist, return message
        IF column_exists = 0 THEN
            SELECT 'No mcc_err_check data' AS message;
        ELSE
            -- Step 4: Construct and execute the dynamic SQL query
            SET @dyn_sql = CONCAT(
                'SELECT ',
                    'mcc_err_check, ',
                    'COUNT(*) AS fail_count ',
                'FROM `', in_schema, '`.`', in_table, '` ',
                'WHERE mcc_err_check IS NOT NULL AND mcc_err_check != "NA" ',
                'AND wo IN (', in_wo_filter, ') ',
                'AND is_y = 1 ',
                'GROUP BY mcc_err_check ',
                'ORDER BY fail_count DESC'
            );

            PREPARE stmt FROM @dyn_sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
        END IF;
    END IF;
END //

CREATE OR REPLACE PROCEDURE get_mcc_upload_rate_distribution_wo(
    IN in_schema VARCHAR(64),
    IN in_table VARCHAR(64),
    IN in_wo_filter TEXT
)
BEGIN
    DECLARE column_exists INT;

    -- Step 1: Check if the WO filter is empty or null
    IF in_wo_filter IS NULL OR LENGTH(TRIM(in_wo_filter)) = 0 THEN
        SELECT 'No wo selected' AS message;
    ELSE
        SELECT COUNT(*) INTO column_exists
        FROM information_schema.COLUMNS 
        WHERE TABLE_SCHEMA = in_schema 
        AND TABLE_NAME = in_table 
        AND COLUMN_NAME = 'mcc_err_upload';

        -- Step 3: If column doesn't exist, return message
        IF column_exists = 0 THEN
            SELECT 'No mcc_err_upload data' AS message;
        ELSE
            -- Step 4: Construct and execute the dynamic SQL query
            SET @dyn_sql = CONCAT(
                'SELECT ',
                    'mcc_err_upload, ',
                    'COUNT(*) AS fail_count ',
                'FROM `', in_schema, '`.`', in_table, '` ',
                'WHERE mcc_err_upload IS NOT NULL AND mcc_err_upload != "NA" ',
                'AND wo IN (', in_wo_filter, ') ',
                'AND is_y = 1 ',
                'GROUP BY mcc_err_upload ',
                'ORDER BY fail_count DESC'
            );

            PREPARE stmt FROM @dyn_sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
        END IF;
    END IF;
END //