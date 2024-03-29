-- SQLite
CREATE TABLE Fragments (
    FragmentID INTEGER PRIMARY KEY AUTOINCREMENT,
    FileID INTEGER NOT NULL,
    FragmentName VARCHAR(255) NOT NULL,
    FragmentNumber VARCHAR(255) NOT NULL,
    NodeIDs VARCHAR(255) NOT NULL,
    FOREIGN KEY (FileID) REFERENCES Files(FileID)
);
