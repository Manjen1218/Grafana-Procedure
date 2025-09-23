DELIMITER //
CREATE OR REPLACE PROCEDURE `get_jig_heatmap`(
    IN in_db VARCHAR(64), 
    IN in_ts VARCHAR(64),
    IN in_temp VARCHAR(64),
    IN from_time VARCHAR(64), 
    IN to_time VARCHAR(64)
)
BEGIN
    DECLARE temp_expr TEXT;
    DECLARE temp_check_expr TEXT;
    DECLARE rpi_mac_col TEXT;

    -- Convert ISO datetime strings to DATETIME
    SET @from_dt = STR_TO_DATE(SUBSTRING_INDEX(from_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');
    SET @to_dt = STR_TO_DATE(SUBSTRING_INDEX(to_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');

    -- Detect temperature-related columns
    SELECT 
        GROUP_CONCAT(CONCAT('IFNULL(', column_name, ', -9999)') ORDER BY column_name SEPARATOR ', '),
        GROUP_CONCAT(CONCAT(column_name, ' IS NOT NULL') ORDER BY column_name SEPARATOR ' OR ')
    INTO 
        temp_expr,
        temp_check_expr
    FROM information_schema.columns
    WHERE table_schema = in_db
      AND table_name = in_ts
      AND column_name IN (
          'soc_temp_max', 'critical_temp_max', 'berg5_soc_t_max', 
          'hpl_soc_temp_max', 'mcf_soc_t_max'
      );
    
    -- Check if rpi_mac column exists
    SELECT 
      IF(COUNT(*) > 0, 'rpi_mac', 'NULL AS rpi_mac')
    INTO 
      rpi_mac_col
    FROM 
      information_schema.columns
    WHERE 
      table_schema = in_db
      AND table_name = in_ts
      AND column_name = 'rpi_mac';

    -- Fallback if no temp columns are present
    IF temp_expr IS NOT NULL AND temp_expr != '' AND temp_check_expr IS NOT NULL AND temp_check_expr != '' THEN
        SET @temp_column_expr = CONCAT(
            "NULLIF(CASE WHEN (", temp_check_expr, ") ",
            "THEN GREATEST(", temp_expr, ") ELSE -9999 END, -9999) AS temp_val"
        );
    ELSE
        SET @temp_column_expr = "-9999 AS temp_val";
    END IF;

    -- Build full SQL statement dynamically
    SET @sql_query = CONCAT(
        "SELECT  
            jig,
            wo,
            sn,
            err_msg, ",
            rpi_mac_col,
            ", tbeg,
            tend, ",
            @temp_column_expr, " ",
        "FROM ", in_db, ".", in_ts, " ",
        "WHERE is_y = 1 ",
        "AND tbeg BETWEEN '", @from_dt, "' AND '", @to_dt, "' ",
        "AND (",
            "'", in_temp, "' = 'All' OR ",
            "('", in_temp, "' = 'Above 90°C' AND soc_temp_max > 90) OR ",
            "('", in_temp, "' = 'Above 80°C' AND soc_temp_max > 80) OR ",
            "('", in_temp, "' = 'Above 70°C' AND soc_temp_max > 70) OR ",
            "('", in_temp, "' = 'Above 60°C' AND soc_temp_max > 60) OR ",
            "('", in_temp, "' = 'Above 50°C' AND soc_temp_max > 50)",
        ")"
    );

    -- Execute the final query
    PREPARE stmt FROM @sql_query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //