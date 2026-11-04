# BPM
```yaml
name: BPM
runs_as: BPM
database: BPM
admin_conn: '@DB_DRIVER_NAME:@DB_DSN'
```

## ST001
```yaml
name: Department EI
desc: Department responsible for initial review
focal_points:
  - name: Example Contact
    email: contact@example.com
    role: Process Owner
```

## BP001
```yaml
name: New Supplier Onboarding
desc: Review and approve a new supplier
version: v1.0.0
process_status: draft
steps_orientation: horizontal
```

## SP001
```yaml
name: Request received
desc: The supplier onboarding request enters the process
```

## BA001
```yaml
name: Capture supplier details
desc: Collect the information required to assess the supplier
actors:
  - ST001
data_schema:
  - name: supplier_name
    label: Supplier name
    type: varchar
    desc: Registered name of the supplier
    nullable: false
    size: 6
    order_index: 1
  - name: tax_registered
    label: Tax registered
    type: bool
    desc: Whether the supplier has a valid tax registration
    nullable: false
    size: 3
    order_index: 2
action_buttons:
  - name: verify_supplier
    label: Verify supplier
    type: api
    api: /api/suppliers/verify
```

## DP001
```yaml
name: Supplier eligible?
desc: Route the process based on the submitted tax registration
```

## BA002
```yaml
name: Request additional information
desc: Ask the requester to correct or complete the supplier details
actors:
  - ST001
data_schema:
  - name: correction_notes
    label: Correction notes
    type: text
    desc: Information the requester must provide
    nullable: false
    size: 12
    order_index: 1
```

## BA003
```yaml
name: Approve supplier
desc: Complete the supplier onboarding review
actors:
  - ST001
```

## EP001
```yaml
name: Onboarding complete
desc: Supplier onboarding has reached an end state
```

## FL001
```yaml
flow:
  - SP001->BA001->DP001(BA001.tax_registered=TRUE,BA003,BA002)->EP001
```
