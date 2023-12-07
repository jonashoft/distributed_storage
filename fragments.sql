CREATE TABLE Fragments (
    FragmentID INT AUTO_INCREMENT PRIMARY KEY,
    FileID INT NOT NULL,
    FragmentName VARCHAR(255) NOT NULL,
    NodeIDs VARCHAR(255) NOT NULL,
    FOREIGN KEY (FileID) REFERENCES Files(FileID)
    -- Add more fields if necessary
);