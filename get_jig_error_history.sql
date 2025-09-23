DELIMITER //
CREATE OR REPLACE PROCEDURE `get_jig_error_history`(
    IN from_time VARCHAR(30), 
    IN to_time VARCHAR(30)
)
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE db_name VARCHAR(64);
    DECLARE tbl_name VARCHAR(64);
    DECLARE dyn_sql TEXT;

    DECLARE cur CURSOR FOR
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA LIKE 'k2%' 
        AND TABLE_NAME IN ('pt', 'pts', 'pdlp');

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    SET @from_dt = STR_TO_DATE(SUBSTRING_INDEX(from_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');
    SET @to_dt   = STR_TO_DATE(SUBSTRING_INDEX(to_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');

    -- Temp tables
    DROP TEMPORARY TABLE IF EXISTS tmp_jigs_in_timeframe;
    CREATE TEMPORARY TABLE tmp_jigs_in_timeframe (
        jig VARCHAR(100)
    );

    DROP TEMPORARY TABLE IF EXISTS tmp_all_records;
    CREATE TEMPORARY TABLE tmp_all_records (
        jig VARCHAR(100),
        tbeg TIMESTAMP,
        err_msg TEXT,
        wo VARCHAR(100),
        dbname VARCHAR(64),
        tblname VARCHAR(64)
    );

    DROP TEMPORARY TABLE IF EXISTS tmp_all_records_sorted;
    CREATE TEMPORARY TABLE tmp_all_records_sorted (
        jig VARCHAR(100),
        tbeg TIMESTAMP,
        err_msg TEXT,
        wo VARCHAR(100),
        dbname VARCHAR(64),
        tblname VARCHAR(64)
    );

    DROP TEMPORARY TABLE IF EXISTS tmp_latest_err;
    CREATE TEMPORARY TABLE tmp_latest_err (
        jig VARCHAR(100),
        err_msg TEXT,
        dbname VARCHAR(64),
        tblname VARCHAR(64),
        wo VARCHAR(100)
    );

    DROP TEMPORARY TABLE IF EXISTS tmp_streaks;
    CREATE TEMPORARY TABLE tmp_streaks (
        jig VARCHAR(100),
        err_msg TEXT,
        streak_len INT,
        streak_start_tbeg TIMESTAMP,
        dbname VARCHAR(64),
        tblname VARCHAR(64),
        wo VARCHAR(100)
    );

    -- Step 1: Get jigs with records in the timeframe
    SET done = FALSE;
    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO db_name, tbl_name;
        IF done THEN LEAVE read_loop; END IF;

        SET @dyn_sql = CONCAT(
            'INSERT IGNORE INTO tmp_jigs_in_timeframe (jig) ',
            'SELECT DISTINCT jig FROM ', db_name, '.', tbl_name, ' ',
            'WHERE tbeg BETWEEN ? AND ? AND jig IS NOT NULL AND is_y = 1'
        );

        PREPARE stmt FROM @dyn_sql;
        EXECUTE stmt USING @from_dt, @to_dt;
        DEALLOCATE PREPARE stmt;
    END LOOP;
    CLOSE cur;

    
    -- Step 2: Load full history for those jigs
    SET done = FALSE;
    OPEN cur;
    read_loop2: LOOP
        FETCH cur INTO db_name, tbl_name;
        IF done THEN LEAVE read_loop2; END IF;

        SET @dyn_sql = CONCAT(
            'INSERT INTO tmp_all_records (jig, tbeg, err_msg, wo, dbname, tblname) ',
            'SELECT jig, tbeg, err_msg, wo, "', db_name, '", "', tbl_name, '" FROM ', db_name, '.', tbl_name, ' ',
            'WHERE jig IN (SELECT jig FROM tmp_jigs_in_timeframe) and is_y = 1 ',
            'AND tbeg < ?'
        );

        PREPARE stmt FROM @dyn_sql;
        EXECUTE stmt USING @to_dt;
        DEALLOCATE PREPARE stmt;
    END LOOP;
    CLOSE cur;

    DROP TEMPORARY TABLE IF EXISTS tmp_all_records_sorted;
    CREATE TEMPORARY TABLE tmp_all_records_sorted AS
    SELECT * FROM tmp_all_records
    ORDER BY jig, tbeg DESC;

        -- Step 3: Get the latest error message per jig
    DELETE FROM tmp_latest_err;
    INSERT INTO tmp_latest_err (jig, err_msg, dbname, tblname, wo)
    SELECT r.jig, r.err_msg, r.dbname, r.tblname, r.wo
    FROM tmp_all_records_sorted r
    JOIN (
        SELECT jig, MAX(tbeg) AS max_tbeg
        FROM tmp_all_records_sorted
        GROUP BY jig
    ) latest ON r.jig = latest.jig AND r.tbeg = latest.max_tbeg
    GROUP BY r.jig;

    -- Step 4: Calculate streaks
    SET @prev_jig := '';
    SET @target_err := '';
    SET @streak_len := 0;
    SET @streak_finalized := 0;
    SET @streak_start := '9999-12-31 23:59:59';

    DELETE FROM tmp_streaks;

    INSERT INTO tmp_streaks (jig, err_msg, streak_len, streak_start_tbeg, dbname, tblname, wo)
    SELECT
        jig,
        target_err AS err_msg,
        MAX(streak_len) AS streak_len,
        MIN(streak_start_tbeg) AS streak_start_tbeg,
        dbname,
        tblname,
        wo
    FROM
    (SELECT
        r.jig,
        r.tbeg,
        r.err_msg,
        le.err_msg AS target_err,
        le.dbname,
        le.tblname,
        le.wo,
        
        @streak_len := IF(
            r.jig = @prev_jig,
            IF(
                @streak_finalized = 1,
                @streak_len, -- freeze streak if finalized
                IF(r.err_msg = le.err_msg,
                    @streak_len + 1, -- increment streak if same error
                    @streak_len -- freeze streak if error changed
                )
            ),
            1 -- new jig, reset streak to 1
        ) AS streak_len,
        
        @streak_start := IF(
            r.jig = @prev_jig AND r.err_msg = le.err_msg,
            LEAST(@streak_start, r.tbeg),
            IF(r.err_msg = le.err_msg, r.tbeg, NULL)
        ) AS streak_start_tbeg,
        
        @streak_finalized := IF(
            r.jig = @prev_jig AND (r.err_msg IS NULL OR r.err_msg != le.err_msg),
            1, -- finalize streak on error change
            IF(r.jig != @prev_jig, 0, @streak_finalized) -- reset finalize flag on new jig
        ) AS streak_finalized,
        
        @prev_jig := r.jig
    FROM tmp_all_records_sorted r
    JOIN tmp_latest_err le ON r.jig = le.jig) streak WHERE streak_len >= 3 GROUP BY jig;

    SELECT
        jig,
        err_msg,
        streak_len AS count,
        streak_start_tbeg AS start_tbeg,
        dbname AS SKU, 
        tblname AS test_station, 
        wo
    FROM tmp_streaks
    WHERE err_msg IS NOT NULL 
    ORDER BY jig;

    -- Cleanup
    DROP TEMPORARY TABLE IF EXISTS tmp_jigs_in_timeframe;
    DROP TEMPORARY TABLE IF EXISTS tmp_all_records;
    DROP TEMPORARY TABLE IF EXISTS tmp_all_records_sorted;
    DROP TEMPORARY TABLE IF EXISTS tmp_latest_err;
    DROP TEMPORARY TABLE IF EXISTS tmp_streaks;
END //