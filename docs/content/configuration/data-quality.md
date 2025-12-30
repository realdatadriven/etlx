+++
title = 'Data Quality'
weight = 44
draft = false
+++

### Data Quality

The `DATA_QUALITY` section allows you to define and execute validation rules to ensure the quality of your data. Each rule performs a check using a SQL query to identify records that violate a specific condition. Optionally, you can define a query to fix any identified issues automatically if applicable.

---

#### **Data Quality Structure**

1. **Metadata**:
   - The `DATA_QUALITY` section contains metadata describing its purpose and activation status.

2. **Validation Rules**:
   - Each validation rule is defined as a Level 2 heading under the `DATA_QUALITY` block.
   - Rules include a query to check for violations and, optionally, a query to fix issues.

3. **Execution**:
   - The system loops through all rules in the `DATA_QUALITY` block.
   - For each rule:
     - Runs the validation query.
     - If violations are found and a fix query is defined, executes the fix query.

---

(See README for full examples and behavior details.)
