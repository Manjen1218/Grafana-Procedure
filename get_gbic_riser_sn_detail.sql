DELIMITER //

CREATE OR REPLACE PROCEDURE get_gbic_riser_sn_detail(
    IN in_db VARCHAR(64),
    IN gbic_or_riser_col VARCHAR(30),
    IN in_sn VARCHAR(64)
)
BEGIN
    DECLARE done INT DEFAULT 0;
    DECLARE dyn_sql TEXT;
    DECLARE current_table VARCHAR(64);
    DECLARE rpi_mac_col TEXT;
    DECLARE col_count INT DEFAULT 0;

    -- Cursor to iterate over all tables that have the specified column
    DECLARE cur CURSOR FOR
        SELECT TABLE_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = in_db
          AND COLUMN_NAME = gbic_or_riser_col;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    -- Temporary table to hold results
    CREATE TEMPORARY TABLE IF NOT EXISTS temp_results (
        sn VARCHAR(64),
        test_station VARCHAR(64),
        bname VARCHAR(255),
        wo VARCHAR(255),
        tbeg DATETIME,
        tpver VARCHAR(64),
        err_msg TEXT,
        rpi_mac VARCHAR(64),
        fullpath TEXT
    );

    OPEN cur;

    read_loop: LOOP
        FETCH cur INTO current_table;
        IF done THEN
            LEAVE read_loop;
        END IF;

        -- Check if 'rpi_mac' column exists in current table
        SELECT COUNT(*)
        INTO col_count
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = in_db
          AND TABLE_NAME = current_table
          AND COLUMN_NAME = 'rpi_mac';

        -- Build the SELECT part for rpi_mac conditionally
        IF col_count > 0 THEN
            SET rpi_mac_col = 'rpi_mac';
        ELSE
            SET rpi_mac_col = '\'None\'';
        END IF;

        -- Construct the dynamic SQL for current table
        SET dyn_sql = CONCAT(
            'INSERT INTO temp_results ',
            'SELECT sn, ''', current_table, ''' AS test_station, bname, wo, tbeg, tpver, err_msg, ', rpi_mac_col, ' AS rpi_mac, fullpath ',
            'FROM ', in_db, '.', current_table, ' ',
            'WHERE ', gbic_or_riser_col, ' = ? AND is_y = 1'
        );

        PREPARE stmt FROM dyn_sql;
        EXECUTE stmt USING in_sn;
        DEALLOCATE PREPARE stmt;
    END LOOP;

    CLOSE cur;

    -- Return the result
    SELECT * FROM temp_results;

    -- Clean up
    DROP TEMPORARY TABLE IF EXISTS temp_results;
END //

DELIMITER ;
