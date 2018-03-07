CREATE TABLE `hdfs_storages` (
  `host_id` varchar(255) NOT NULL,
  `storage_id` int(11) NOT NULL,
  `storage_type` int(11) NOT NULL,
  PRIMARY KEY (`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;

ALTER TABLE `hdfs_inodes` ADD COLUMN `storage_policy` bit(8) NOT NULL DEFAULT '0';

ALTER TABLE `hdfs_inodes` ADD COLUMN `has_ace_1` tinyint NOT NULL DEFAULT '0';
ALTER TABLE `hdfs_inodes` ADD COLUMN `has_ace_2` tinyint NOT NULL DEFAULT '0';
ALTER TABLE `hdfs_inodes` ADD COLUMN `has_more_aces` tinyint NOT NULL DEFAULT '0';

CREATE TABLE `hdfs_aces` (
  `inode_id` int(11) NOT NULL,
  `index` int(11) NOT NULL,
  `subject` VARCHAR(100) NOT NULL,
  `type` int NOT NULL DEFAULT '0',
  `is_default` tinyint NOT NULL DEFAULT '0',
  `permission` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`inode_id`,`index`),
  KEY `inode_idx` (`index`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs COMMENT='NDB_TABLE=READ_BACKUP=1'
  /*!50100 PARTITION BY KEY (inode_id) */