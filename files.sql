CREATE TABLE Files (
    FileID INT AUTO_INCREMENT PRIMARY KEY,
    Filename VARCHAR(255) NOT NULL,
    Size BIGINT NOT NULL,
    ContentType VARCHAR(50) NOT NULL,
    CreatedTime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Add more fields if necessary
);