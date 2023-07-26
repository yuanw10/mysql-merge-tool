-- Following queries are supposed to be executed on target database to match updates on source database:
-- Create new tables:
CREATE TABLE `owner` (
  `id` int NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Drop deleted tables:
DROP TABLE dogs;

-- Modify structures of updated tables:
ALTER TABLE cats ADD `owner_id` int DEFAULT NULL;
ALTER TABLE cats ADD `birth` date DEFAULT NULL;
ALTER TABLE cats ADD `gender` char(1) DEFAULT NULL;
ALTER TABLE cats MODIFY `name` varchar(10) DEFAULT NULL;
ALTER TABLE cats MODIFY `id` int unsigned NOT NULL AUTO_INCREMENT;
ALTER TABLE cats DROP new_col_target;
-- Errors might be thrown if constraints are complicated
-- Double check when executing
ALTER TABLE cats ADD CONSTRAINT `owner_id_fk` FOREIGN KEY (`owner_id`) REFERENCES `owner` (`id`);
ALTER TABLE cats ADD KEY `owner_id_fk_idx` (`owner_id`);
ALTER TABLE cats ADD UNIQUE KEY `name_UNIQUE` (`name`);

