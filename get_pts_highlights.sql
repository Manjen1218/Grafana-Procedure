DELIMITER //

CREATE OR REPLACE PROCEDURE get_pts_highlights(
    IN in_db VARCHAR(64),
    IN from_time VARCHAR(30), 
    IN to_time VARCHAR(30)
)
BEGIN
    DECLARE table_count INT DEFAULT 0;
    DECLARE dyn_sql TEXT;

    SET @from_dt = STR_TO_DATE(SUBSTRING_INDEX(from_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');
    SET @to_dt = STR_TO_DATE(SUBSTRING_INDEX(to_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');

    -- Step 1: Check if pts table exists in the given schema
    SELECT COUNT(*) INTO table_count
    FROM information_schema.tables
    WHERE table_schema = in_db AND table_name = 'pts';

    IF table_count = 0 THEN
        SELECT 'No PTS table available' AS message;
    ELSE
        -- Step 2: Prepare dynamic SQL
        SET @dyn_sql = CONCAT(
            'SELECT ',
                'p.wo, ',
                'p.sn, ',
                'p.tbeg, ',
                'p.err_msg, ',
                'p.err_id, ',
                'CASE WHEN p.err_msg IS NULL THEN 1 ELSE 0 END AS flag ',
            'FROM ', in_db, '.pts p ',
            'JOIN ( ',
                'SELECT sn, MAX(tbeg) AS max_tbeg ',
                'FROM ', in_db, '.pts ',
                'WHERE tbeg BETWEEN ? AND ? AND is_y = 1 ',
                'GROUP BY sn ',
            ') latest ON p.sn = latest.sn AND p.tbeg = latest.max_tbeg ',
            'WHERE EXISTS ( ',
                'SELECT 1 ',
                'FROM ', in_db, '.pts p2 ',
                'WHERE p2.sn = p.sn ',
                'AND p2.err_id LIKE ''s%'' ',
                'AND p2.tbeg BETWEEN ? AND ? ',
                'AND p2.is_y = 1 ',
            ')'
        );

        -- Step 3: Prepare and execute the statement
        PREPARE stmt FROM @dyn_sql;
        EXECUTE stmt USING @from_dt, @to_dt, @from_dt, @to_dt;
        DEALLOCATE PREPARE stmt;
    END IF;
END //

DELIMITER ;
