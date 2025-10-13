-- Create a table
CREATE TABLE employees (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    position VARCHAR(50),
    salary DECIMAL(10, 2)
);

-- Insert data
INSERT INTO employees (id, name, position, salary) VALUES
(1, 'Alice Smith', 'Developer', 75000.00),
(2, 'Bob Johnson', 'Manager', 90000.00);

-- Select data
SELECT * FROM employees;

-- Update data
UPDATE employees SET salary = 80000.00 WHERE id = 1;

-- Delete data
DELETE FROM employees WHERE id = 2;