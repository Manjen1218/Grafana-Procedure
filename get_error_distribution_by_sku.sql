DELIMITER //
CREATE OR REPLACE PROCEDURE `get_error_distribution_by_sku`(
    IN in_db VARCHAR(100),
    IN in_ts_filter TEXT, 
    IN from_time VARCHAR(30), 
    IN to_time VARCHAR(30)
)
BEGIN
    DECLARE done INT DEFAULT 0;
    DECLARE tbl_name VARCHAR(64);
    DECLARE first_table BOOLEAN DEFAULT TRUE;

    -- Cursor to loop through selected tables that exist in the schema
    DECLARE tbl_cursor CURSOR FOR
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = in_db
        AND table_name IN (
            SELECT TRIM(val)
            FROM JSON_TABLE(
                CONCAT('["', REPLACE(in_ts_filter, ',', '","'), '"]'),
                '$[*]' COLUMNS(val VARCHAR(64) PATH '$')
            ) AS jt
        );

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
    SET @from_dt = STR_TO_DATE(SUBSTRING_INDEX(from_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');
    SET @to_dt = STR_TO_DATE(SUBSTRING_INDEX(to_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');
    

    SET @union_sql = '';

    OPEN tbl_cursor;
    read_loop: LOOP
        FETCH tbl_cursor INTO tbl_name;
        IF done THEN
            LEAVE read_loop;
        END IF;

        IF first_table THEN
            SET @union_sql = CONCAT(@union_sql,
                'SELECT ''', in_db, ''' AS sku_filter, ''', tbl_name, ''' AS test_station, wo, sn, err_msg, err_id, tbeg FROM ', in_db, '.', tbl_name, 
                " WHERE tbeg BETWEEN '", @from_dt, "' AND '", @to_dt, "' AND is_y = 1");
            SET first_table = FALSE;
        ELSE
            SET @union_sql = CONCAT(@union_sql,
                ' UNION ALL SELECT ''', in_db, ''' AS sku_filter, ''', tbl_name, ''' AS test_station, wo, sn, err_msg, err_id, tbeg FROM ', in_db, '.', tbl_name, 
                " WHERE tbeg BETWEEN '", @from_dt, "' AND '", @to_dt, "' AND is_y = 1");
        END IF;
    END LOOP;
    CLOSE tbl_cursor;

    -- If we found any valid tables, run the query
    IF @union_sql != '' THEN
    SET @final_sql = CONCAT('
        WITH combined AS (
            ', @union_sql, '
        ),
        station_totals AS (
            SELECT test_station, COUNT(*) AS total_records
            FROM combined
            GROUP BY test_station
        ),
        filtered AS (
            SELECT sku_filter, test_station, err_msg, err_id
            FROM combined
            WHERE err_msg IS NOT NULL
        ),
        aggregated AS (
            SELECT sku_filter, test_station, err_msg, err_id, COUNT(*) AS count
            FROM filtered
            GROUP BY sku_filter, test_station, err_id, err_msg
        )
        SELECT
            err_msg AS label,
            err_id, 
            count AS value
        FROM aggregated a
        JOIN station_totals st
            ON a.test_station = st.test_station
        ORDER BY a.sku_filter, a.test_station, count DESC;
    ');

        PREPARE stmt FROM @final_sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    ELSE
        SELECT 'No valid test tables found in schema' AS message;
    END IF;
END //