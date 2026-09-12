# Output preview lifecycle

Capture per-table input snapshots, execute base SQL, execute candidate SQL, compare outputs, save the report, and remove owned references. The report remains available after cleanup. Production deployment is separate. Input capture is not an atomic cross-table transaction. Snapshot retention and safe physical file reclamation require Iceberg maintenance.
