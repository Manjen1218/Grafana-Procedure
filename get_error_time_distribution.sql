DELIMITER //

CREATE OR REPLACE PROCEDURE get_error_time_distribution (
    IN in_db VARCHAR(64),
    IN in_ts VARCHAR(64),
    IN in_fwver VARCHAR(64),
    IN in_interval_hours INT
)
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE v_tbeg DATETIME;
    DECLARE v_err_msg TEXT;
    DECLARE i INT;
    DECLARE msg_part TEXT;
    DECLARE clean_msg TEXT;
    DECLARE split_msg_part TEXT;

    -- Step 0: Declare cursor
    DECLARE cur CURSOR FOR SELECT tbeg, err_msg FROM temp_source;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    -- Step 1: Create temp tables
    DROP TEMPORARY TABLE IF EXISTS temp_source;
    CREATE TEMPORARY TABLE temp_source (
        tbeg DATETIME,
        err_msg TEXT
    );

    DROP TEMPORARY TABLE IF EXISTS temp_parsed;
    CREATE TEMPORARY TABLE temp_parsed (
        tbeg DATETIME,
        tbucket DATETIME,
        parsed_msg TEXT
    );

    -- Step 2: Load data dynamically
    SET @sql := CONCAT('INSERT INTO temp_source SELECT tbeg, err_msg FROM `', in_db, '`.`', in_ts, '` WHERE err_msg IS NOT NULL AND is_y = 1 AND fwver IN (', in_fwver, ")");
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    -- Step 3: Parse error messages
    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO v_tbeg, v_err_msg;
        IF done THEN
            LEAVE read_loop;
        END IF;

        -- Remove the " Fail" string from the error message
        SET clean_msg = REPLACE(v_err_msg, ' Fail', '');

        -- Split by comma
        SET i = 1;
        WHILE i <= CHAR_LENGTH(clean_msg) - CHAR_LENGTH(REPLACE(clean_msg, ',', '')) + 1 DO
            SET msg_part = TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(clean_msg, ',', i), ',', -1));

            -- For each part, split by ":" and take the first element
            IF INSTR(msg_part, ':') > 0 THEN
                SET split_msg_part = SUBSTRING_INDEX(msg_part, ':', 1);
            ELSE
                SET split_msg_part = msg_part;
            END IF;

            -- Insert the parsed message into the temp_parsed table if it's not empty and has a length greater than 3
            IF split_msg_part <> '' AND LENGTH(split_msg_part) > 3 THEN
                SET @rounded_tbeg = DATE_ADD(
                    DATE(v_tbeg),
                    INTERVAL FLOOR(HOUR(v_tbeg) / in_interval_hours) * in_interval_hours HOUR
                );
                INSERT INTO temp_parsed (tbeg, tbucket, parsed_msg)
                VALUES (v_tbeg, @rounded_tbeg, split_msg_part);
            END IF;

            SET i = i + 1;
        END WHILE;
    END LOOP;
    CLOSE cur;

    -- Step 4: Build the dynamic pivot query
    SET @col_sql = '';
    SELECT GROUP_CONCAT(DISTINCT
        CONCAT(
            'MAX(CASE WHEN parsed_msg = ''',
            REPLACE(parsed_msg, '''', ''''''),  -- escape single quotes
            ''' THEN cnt ELSE NULL END) AS `',
            REPLACE(parsed_msg, '`', ''),       -- remove backticks from alias
            '`'
        )
        ORDER BY parsed_msg
    )
    INTO @col_sql
    FROM temp_parsed;

    SET @final_sql = CONCAT(
        'SELECT tbucket AS time, ', @col_sql,
        ' FROM (',
        '  SELECT tbucket, parsed_msg, COUNT(*) AS cnt FROM temp_parsed GROUP BY tbucket, parsed_msg',
        ') AS base ',
        'GROUP BY tbucket ORDER BY tbucket'
    );

    -- Step 5: Execute pivoted query
    PREPARE stmt FROM @final_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END;
//
DELIMITER ;
