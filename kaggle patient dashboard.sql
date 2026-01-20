-- Convert _date columns to datetime format
DELIMITER $$

DROP PROCEDURE IF EXISTS ConvertTextDateColumnsToDateTime$$
CREATE PROCEDURE ConvertTextDateColumnsToDateTime(
    IN target_db VARCHAR(255),
    IN date_format VARCHAR(100)
)
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE tbl VARCHAR(255);
    DECLARE col VARCHAR(255);

    DECLARE cur CURSOR FOR
      SELECT c.TABLE_NAME, c.COLUMN_NAME
      FROM INFORMATION_SCHEMA.COLUMNS AS c
      JOIN INFORMATION_SCHEMA.TABLES AS t
        ON c.TABLE_SCHEMA = t.TABLE_SCHEMA
       AND c.TABLE_NAME = t.TABLE_NAME
      WHERE c.TABLE_SCHEMA = target_db
        AND c.COLUMN_NAME LIKE '%_date%'
        AND c.DATA_TYPE IN ('varchar','text','char')
        AND t.TABLE_TYPE = 'BASE TABLE';      -- only base tables

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO tbl, col;
        IF done THEN
            LEAVE read_loop;
        END IF;

        SET @orig_table = CONCAT('`', target_db, '`.`', tbl, '`');
        SET @orig_col   = col;
        -- make unique temp name per table+column
        SET @tmp_col    = CONCAT(tbl, '_', col, '_dt');

        -- add temp datetime column (if not exists)
        IF NOT EXISTS (
            SELECT 1
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = target_db
              AND TABLE_NAME = tbl
              AND COLUMN_NAME = @tmp_col
        ) THEN
            SET @sql = CONCAT(
              'ALTER TABLE ', @orig_table,
              ' ADD COLUMN `', @tmp_col, '` DATETIME;'
            );
            PREPARE stmt FROM @sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
        END IF;

        -- convert text values
        SET @sql = CONCAT(
          'UPDATE ', @orig_table,
          ' SET `', @tmp_col, '` = STR_TO_DATE(`', @orig_col,
          '`, "', date_format, '");'
        );
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- drop original column
        SET @sql = CONCAT(
          'ALTER TABLE ', @orig_table,
          ' DROP COLUMN `', @orig_col, '`;'
        );
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- rename temp column to original
        SET @sql = CONCAT(
          'ALTER TABLE ', @orig_table,
          ' CHANGE COLUMN `', @tmp_col,
          '` `', @orig_col, '` DATETIME;'
        );
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

    END LOOP;

    CLOSE cur;
END$$

DELIMITER ;

CALL ConvertAllDateTextToDateTimeDB('kaggle_patient_journey_processed','%d-%m-%Y %H:%i');

-- Find duplicates in encounters
DELIMITER $$

DROP PROCEDURE IF EXISTS CheckDuplicateRowsForTables$$

CREATE PROCEDURE CheckDuplicateRowsForTables(
    IN target_db   VARCHAR(255),
    IN table_list  TEXT
)
BEGIN
    DECLARE current_tbl VARCHAR(255);

    -- Trim any spaces
    SET table_list = TRIM(table_list);

    check_loop: LOOP
        -- If list is empty, exit
        IF table_list = '' THEN
            LEAVE check_loop;
        END IF;

        -- Extract the next table name
        SET current_tbl = TRIM(
            SUBSTRING_INDEX(table_list, ',', 1)
        );

        -- Build full table ref
        SET @full_table = CONCAT('`', target_db, '`.`', current_tbl, '`');

        -- Run duplicate-check using distinct vs total count
        SET @sql = CONCAT(
            'SELECT ''', current_tbl, ''' AS table_name, ',
            'CASE WHEN total_rows > distinct_rows ',
            'THEN ''DUPLICATES FOUND'' ELSE ''NO DUPLICATES'' END AS status ',
            'FROM (',
            'SELECT ',
            '(SELECT COUNT(*) FROM ', @full_table, ') AS total_rows, ',
            '(SELECT COUNT(*) FROM (SELECT DISTINCT * FROM ', @full_table, ') AS dt) AS distinct_rows ',
            ') AS stats;'
        );

        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- Remove the part processed + comma, ready for next
        IF LOCATE(',', table_list) > 0 THEN
            SET table_list = TRIM(
                SUBSTRING(table_list, LOCATE(',', table_list) + 1)
            );
        ELSE
            SET table_list = '';
        END IF;

    END LOOP check_loop;

END$$

DELIMITER ;

CALL CheckDuplicateRowsForTables(
    'kaggle_patient_journey_processed', 
    'encounters,claims_and_billing,providers,patients'
);
-- No dups so no further process was implemented


create view procedure_billing as (
with bp_cte as (
	select cb.*, 
    pr.procedure_id,
    pr.procedure_code,
    pr.procedure_description,
    pr.procedure_date,
    pr.procedure_cost,
    pr.provider_id
    from procedures pr 
	join claims_and_billing cb using (encounter_id)
)
SELECT
  bp_cte.*,
  ROUND(
    SUM(procedure_cost) OVER (PARTITION BY encounter_id), 2) 
    AS total_procedure_cost_by_encounter, pv.specialty
FROM bp_cte
join providers pv using (provider_id));


create view total_appt_costs as (
with proc_cte as (
	select distinct p.encounter_id, 
	round(SUM(procedure_cost) OVER (PARTITION BY p.encounter_id),2) as app_proc_cost
    from procedures p),
    med_cte as (
    select distinct m.encounter_id, round(SUM(m.cost) OVER (PARTITION BY m.encounter_id),2) as app_med_cost from medications m)
select proc_cte.encounter_id as encounter_id, proc_cte.app_proc_cost, med_cte.app_med_cost
from proc_cte
left join med_cte on proc_cte.encounter_id = med_cte.encounter_id);    

    
