CREATE TABLE `users` (
  id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  name varchar(128) NOT NULL,
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
