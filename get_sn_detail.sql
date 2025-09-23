DELIMITER //

CREATE OR REPLACE PROCEDURE `get_sn_detail`(
  IN in_db VARCHAR(64),
  IN in_sn VARCHAR(64)
)
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE v_table_name VARCHAR(64);
    DECLARE v_col_name VARCHAR(64);
    DECLARE sql_fragment TEXT DEFAULT '';
    DECLARE sql_text TEXT DEFAULT '';
    DECLARE col_list TEXT DEFAULT '';
    
    DECLARE cur CURSOR FOR 
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = in_db
          AND table_name IN ('baking_i', 'baking_o', 'pt', 'pts', 'pdlp');

    DECLARE col_cur CURSOR FOR SELECT col_name FROM tmp_columns;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    -- Create temp table of optional columns
    CREATE TEMPORARY TABLE tmp_columns (col_name VARCHAR(64));
    INSERT INTO tmp_columns (col_name) VALUES 
        ('rpi_mac'), ('fan_rpm1'), ('fan_rpm2'), ('fan_rpm3'), ('fan_rpm4');

    OPEN cur;

    read_loop: LOOP
        FETCH cur INTO v_table_name;
        IF done THEN
            LEAVE read_loop;
        END IF;

        -- Reset for new table
        SET col_list = '';
        SET done = FALSE; -- reset for col cursor

        OPEN col_cur;
        col_loop: LOOP
            FETCH col_cur INTO v_col_name;
            IF done THEN
                SET done = FALSE; -- reset for outer loop
                LEAVE col_loop;
            END IF;

            IF EXISTS (
                SELECT 1 
                FROM information_schema.columns 
                WHERE table_schema = in_db 
                AND table_name = v_table_name 
                AND column_name = v_col_name
            ) THEN
                SET col_list = CONCAT(col_list, ', ', v_col_name);
            ELSE
                SET col_list = CONCAT(col_list, ', NULL AS ', v_col_name);
            END IF;
        END LOOP;
        CLOSE col_cur;

        SET sql_fragment = CONCAT(
            'SELECT sn, "', v_table_name, '" AS test_station, bname, wo, tbeg, tpver, err_msg',
            col_list,
            ', fullpath FROM ', in_db, '.', v_table_name,
            ' WHERE sn = ''', in_sn, ''' AND is_y = 1'
        );

        IF LENGTH(sql_text) > 0 THEN
            SET sql_text = CONCAT(sql_text, ' UNION ALL ', sql_fragment);
        ELSE
            SET sql_text = sql_fragment;
        END IF;
    END LOOP;

    CLOSE cur;
    DROP TEMPORARY TABLE IF EXISTS tmp_columns;

    IF LENGTH(sql_text) > 0 THEN
        SET sql_text = CONCAT(sql_text, ' ORDER BY tbeg');
        PREPARE stmt FROM sql_text;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    ELSE
        SELECT 'No relevant tables found in database' AS msg;
    END IF;
END //

DELIMITER ;
