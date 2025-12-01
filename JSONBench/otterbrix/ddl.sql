-- Create document_table storage for Bluesky data
-- document_table automatically extracts JSON paths and creates columns dynamically
CREATE TABLE bluesky() WITH (storage='document_table');

