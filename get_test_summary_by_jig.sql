DELIMITER //
CREATE OR REPLACE PROCEDURE `get_test_summary_by_jig`(
    IN in_db VARCHAR(64),
    IN in_tables TEXT,
    IN in_jig_prefixes TEXT,
    IN in_jig_suffixes TEXT,
    IN from_time VARCHAR(30), 
    IN to_time VARCHAR(30)
)
BEGIN
    DECLARE pos INT DEFAULT 1;
    DECLARE tbl_count INT;
    DECLARE current_table VARCHAR(64);
    DECLARE comma_pos INT DEFAULT 0;
    DECLARE next_comma_pos INT;
    DECLARE union_sql TEXT DEFAULT '';
    

    -- Count how many tables in in_tables CSV
    SET tbl_count = LENGTH(in_tables) - LENGTH(REPLACE(in_tables, ',', '')) + 1;
    SET @from_dt = STR_TO_DATE(SUBSTRING_INDEX(from_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');
    SET @to_dt = STR_TO_DATE(SUBSTRING_INDEX(to_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');

    WHILE pos <= tbl_count DO
      -- Find the current table name
      IF pos = 1 THEN
        SET next_comma_pos = LOCATE(',', in_tables);
        IF next_comma_pos = 0 THEN
          SET current_table = TRIM(in_tables);
        ELSE
          SET current_table = TRIM(SUBSTRING(in_tables, 1, next_comma_pos - 1));
        END IF;
      ELSE
        SET comma_pos = LOCATE(',', in_tables, comma_pos + 1);
        SET next_comma_pos = LOCATE(',', in_tables, comma_pos + 1);
        IF next_comma_pos = 0 THEN
          SET current_table = TRIM(SUBSTRING(in_tables, comma_pos + 1));
        ELSE
          SET current_table = TRIM(SUBSTRING(in_tables, comma_pos + 1, next_comma_pos - comma_pos - 1));
        END IF;
      END IF;

      -- Append SELECT for current table with table name as test_station
      SET union_sql = CONCAT(
        union_sql,
        IF(union_sql = '', '', ' UNION ALL '),
        'SELECT jig, tbeg, err_msg, ''', current_table, ''' AS test_station FROM ', in_db, '.', current_table,
        " WHERE tbeg BETWEEN '", @from_dt, "' AND '", @to_dt, "' and is_y = 1"
      );

      SET pos = pos + 1;
    END WHILE;

    SET @sql = CONCAT('
      WITH RECURSIVE
      split_prefix AS (
        SELECT 
          TRIM(SUBSTRING_INDEX(', QUOTE(in_jig_prefixes), ', '','', 1)) AS prefix,
          SUBSTRING(', QUOTE(in_jig_prefixes), ', LENGTH(SUBSTRING_INDEX(', QUOTE(in_jig_prefixes), ', '','', 1)) + 2) AS rest
        UNION ALL
        SELECT
          TRIM(SUBSTRING_INDEX(rest, '','', 1)),
          SUBSTRING(rest, LENGTH(SUBSTRING_INDEX(rest, '','', 1)) + 2)
        FROM split_prefix
        WHERE rest <> ''''
      ),
      split_suffix AS (
        SELECT 
          TRIM(SUBSTRING_INDEX(', QUOTE(in_jig_suffixes), ', '','', 1)) AS suffix,
          SUBSTRING(', QUOTE(in_jig_suffixes), ', LENGTH(SUBSTRING_INDEX(', QUOTE(in_jig_suffixes), ', '','', 1)) + 2) AS rest
        UNION ALL
        SELECT
          TRIM(SUBSTRING_INDEX(rest, '','', 1)),
          SUBSTRING(rest, LENGTH(SUBSTRING_INDEX(rest, '','', 1)) + 2)
        FROM split_suffix
        WHERE rest <> ''''
      ),
      combos AS (
        SELECT CONCAT(prefix, ''-'', suffix) AS combo
        FROM split_prefix CROSS JOIN split_suffix
      ),
      all_data AS (
        ', union_sql, '
      ),
      last_errors AS (
      SELECT
        jig,
        test_station,
        MAX(CASE WHEN rn = 1 THEN err_msg END) AS last_err_1,
        MAX(CASE WHEN rn = 2 THEN err_msg END) AS last_err_2,
        MAX(CASE WHEN rn = 3 THEN err_msg END) AS last_err_3,
        CASE
          WHEN 
            MAX(CASE WHEN rn = 1 THEN err_msg END) IS NOT NULL
            AND MAX(CASE WHEN rn = 1 THEN err_msg END) = MAX(CASE WHEN rn = 2 THEN err_msg END)
            AND MAX(CASE WHEN rn = 2 THEN err_msg END) = MAX(CASE WHEN rn = 3 THEN err_msg END)
          THEN 1
          ELSE 0
        END AS highlight_row
      FROM (
        SELECT
          jig,
          test_station,
          err_msg,
          tbeg,
          ROW_NUMBER() OVER (PARTITION BY jig, test_station ORDER BY tbeg DESC) AS rn
        FROM all_data
      ) numbered
      WHERE rn <= 3
      GROUP BY jig, test_station
    )
    SELECT
      d.jig,
      d.test_station,
      COUNT(CASE WHEN d.err_msg IS NULL THEN 1 END) AS pass_count,
      COUNT(CASE WHEN d.err_msg IS NOT NULL THEN 1 END) AS fail_count,
      COUNT(*) AS total,
      ROUND(100.0 * COUNT(CASE WHEN d.err_msg IS NOT NULL THEN 1 END) / COUNT(*), 2) AS fail_percentage,
      le.last_err_1,
      le.last_err_2,
      le.last_err_3,
      le.highlight_row
    FROM all_data d
    LEFT JOIN last_errors le ON le.jig = d.jig AND le.test_station = d.test_station
    WHERE d.jig IN (SELECT combo FROM combos)
      AND d.tbeg BETWEEN ''', @from_dt, ''' AND ''', @to_dt, '''
    GROUP BY d.jig, d.test_station, le.last_err_1, le.last_err_2, le.last_err_3, le.highlight_row
    ORDER BY d.jig, d.test_station
    ');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //