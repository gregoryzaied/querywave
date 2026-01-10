CREATE TABLE employees (
    emp_id SERIAL PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    branch_id INT NOT NULL,
    salary INT
);

CREATE TABLE branches (
    branch_id SERIAL PRIMARY KEY,
    branch_name TEXT NOT NULL,
    location TEXT
);

ALTER TABLE employees
ADD CONSTRAINT fk_branch
FOREIGN KEY (branch_id) REFERENCES branches(branch_id);
