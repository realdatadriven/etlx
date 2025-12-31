+++
title = 'runs_as override'
weight = 63
draft = false
+++

# Advanced Workflow Execution: `runs_as` Override

The `runs_as` field in the metadata block of any Level 1 key allows ETLX to treat a custom section as a built-in block (like `ETL`, `EXPORTS`, etc.), enabling advanced chaining of processes within the same configuration.

---

````md

# ETL_AFTER_SOME_KEY

```yaml metadata
runs_as: ETL
description: "Post-validation data transformation"
active: true
```

## ETL_OVER_SOME_MAIN_STEP

...

````

In this example:

- ETLX will run the original `ETL` block.
- Then execute `DATA_QUALITY`, an so on.
- Then treat `ETL_AFTER_SOME_KEY` as another `ETL` block (since it contains `runs_as: ETL`) and execute it as such.

This allows chaining of processes within the same configuration file.

---

# **⚠️ Order Matters**

The custom section (e.g. `# ETL_AFTER_SOME_KEY`) is executed **in the order it appears** in the Markdown file after the main keys. That means the flow becomes:

1. `# ETL`
2. `# DATA_QUALITY`
3. `# ETL2` (runs as `ETL`)

This enables advanced chaining like:

- Exporting logs after validation.
- Reapplying transformations based on data quality feedback.
- Generating post-validation reports.
