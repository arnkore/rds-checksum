-- 创建测试数据库
DROP DATABASE IF EXISTS test;
CREATE DATABASE test;
USE test;

-- 创建测试表
CREATE TABLE test_table (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255),
    value INT NOT NULL
);

-- 插入测试数据
INSERT INTO test_table (name, value) VALUES 
    ('test1', 100),
    ('test2', 200),
    ('test3', 300),
    ('test4', 400),
    ('test5', 500),
    ('test with spaces', 600),
    ('special!@#$%^&*()chars', 700),
    ('', 800);

-- 注意：non_existent_table 不需要创建，因为它用于测试表不存在的情况 