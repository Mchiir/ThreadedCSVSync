CREATE TABLE IF NOT EXISTS people2 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(250),
    email VARCHAR(100),
    address VARCHAR(100),
    age INT
    );

CREATE TABLE IF NOT EXISTS people3 LIKE people2;