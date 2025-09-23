DELIMITER //

CREATE OR REPLACE PROCEDURE get_error_occurrences_by_failure_types(
    IN in_schema VARCHAR(64),
    IN in_table VARCHAR(64),
    IN in_failure_types TEXT,
    IN from_time VARCHAR(30),
    IN to_time VARCHAR(30)
)
BEGIN
    DECLARE dyn_sql TEXT;

    SET @from_dt = STR_TO_DATE(SUBSTRING_INDEX(from_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');
    SET @to_dt = STR_TO_DATE(SUBSTRING_INDEX(to_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');

    -- Step 1: Check if failure types list is empty or NULL
    IF in_failure_types IS NULL OR LENGTH(TRIM(in_failure_types)) = 0 THEN
        SELECT 'No error available' AS message;
    ELSE
        -- Step 2: Build dynamic SQL
        SET @dyn_sql = CONCAT(
            'SELECT ',
                'tbeg AS test_date, ',
                'COUNT(*) AS fail_count ',
            'FROM ', in_schema, '.', in_table, ' ',
            'WHERE err_msg IS NOT NULL ',
            'AND err_msg IN (', in_failure_types, ') ',
            'AND tbeg BETWEEN ? AND ? ',
            'GROUP BY tbeg ',
            'ORDER BY tbeg'
        );

        -- Step 3: Prepare and execute
        PREPARE stmt FROM @dyn_sql;
        EXECUTE stmt USING @from_dt, @to_dt;
        DEALLOCATE PREPARE stmt;
    END IF;
END //

DELIMITER ;
