DELIMITER //

CREATE OR REPLACE PROCEDURE get_error_occurrences_by_wo(
    IN in_schema VARCHAR(64),
    IN in_table VARCHAR(64),
    IN in_wo_filter TEXT
)
BEGIN
    DECLARE dyn_sql TEXT;

    -- Step 1: Check if the WO filter is empty or null
    IF in_wo_filter IS NULL OR LENGTH(TRIM(in_wo_filter)) = 0 THEN
        SELECT 'No wo selected' AS message;
    ELSE
        -- Step 2: Construct the dynamic SQL query
        SET @dyn_sql = CONCAT(
            'SELECT ',
                'tbeg AS test_date, ',
                'COUNT(*) AS fail_count ',
            'FROM ', in_schema, '.', in_table, ' ',
            'WHERE err_msg IS NOT NULL ',
            'AND wo IN (', in_wo_filter, ') ',
            'AND is_y = 1 ',
            'GROUP BY tbeg ',
            'ORDER BY test_date'
        );

        -- Step 3: Prepare and execute
        PREPARE stmt FROM @dyn_sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;
END //

DELIMITER ;
