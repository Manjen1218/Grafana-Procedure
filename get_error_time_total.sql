DELIMITER //

CREATE OR REPLACE PROCEDURE `get_error_time_total` (
    IN in_db VARCHAR(64),
    IN in_ts VARCHAR(64),
    IN in_fwver VARCHAR(64),
    IN in_interval_hours INT,
    IN from_time VARCHAR(30), 
    IN to_time VARCHAR(30)
)
BEGIN
    -- Convert ISO 8601 timestamps to DATETIME
    SET @from_dt = STR_TO_DATE(SUBSTRING_INDEX(from_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');
    SET @to_dt = STR_TO_DATE(SUBSTRING_INDEX(to_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');

    -- Step 1: Create temp table
    DROP TEMPORARY TABLE IF EXISTS temp_source;
    CREATE TEMPORARY TABLE temp_source (
        tbeg DATETIME,
        err_msg TEXT
    );

    -- Step 2: Load data dynamically with time filter
    SET @sql := CONCAT(
        'INSERT INTO temp_source ',
        'SELECT tbeg, err_msg ',
        'FROM `', in_db, '`.`', in_ts, '` ',
        'WHERE err_msg IS NOT NULL ',
        'AND is_y = 1 ',
        'AND fwver IN (', in_fwver, ') ',
        'AND tbeg BETWEEN ''', @from_dt, ''' AND ''', @to_dt, ''''
    );
    
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    -- Step 3: Return time buckets and error counts in two columns
    SELECT 
        DATE_ADD(
            DATE(tbeg),
            INTERVAL FLOOR(HOUR(tbeg) / in_interval_hours) * in_interval_hours HOUR
        ) AS time_bucket,
        COUNT(*) AS error_count
    FROM temp_source
    GROUP BY time_bucket
    ORDER BY time_bucket;

END;
//
DELIMITER ;