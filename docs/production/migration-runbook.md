# Triple Store to Quad Store Migration Runbook

**Version:** 1.0
**Last Updated:** 2026-01-20
**Expected Downtime:** 1-4 hours (depending on data size)

---

## Table of Contents

1. [Overview](#overview)
2. [Pre-Migration Checks](#2-pre-migration-checks)
3. [Migration Procedure](#3-migration-procedure)
4. [Post-Migration Validation](#4-post-migration-validation)
5. [Rollback Procedures](#5-rollback-procedures)
6. [Emergency Contacts](#6-emergency-contacts)
7. [Estimated Timelines](#7-estimated-timelines)
8. [Troubleshooting](#8-troubleshooting)

---

## Overview

This runbook provides step-by-step instructions for migrating from a triple store to a quad store. The quad store adds named graph support while maintaining backward compatibility with triple data.

### What Changes

| Aspect | Triple Store | Quad Store |
|--------|--------------|------------|
| Indices | 3 (SPO, POS, OSP) | 4 (GSPO, GPOS, SPOG, POSG) |
| Default Graph | Implicit | Explicit (ID: 0) |
| Named Graphs | Not supported | Fully supported |
| Query Format | SPARQL 1.1 | SPARQL 1.1 + GRAPH clauses |

### Migration Strategy

The migration uses a **copy-and-convert** strategy:
1. Triple store remains online during copy
2. New quad store created in separate location
3. Triples converted to quads (assigned to default graph)
4. Validation performed before cutover
5. Application reconfigured to use quad store

---

## 2. Pre-Migration Checks

### 2.1 System Requirements

Verify the target system meets requirements:

```bash
# Check Elixir version
elixir --version  # Should be 1.18+

# Check available disk space (need 2x current database size)
df -h /path/to/storage

# Check available memory
free -h

# Check RocksDB installation
ls /usr/lib/librocksdb.so
```

**Requirements:**
- [ ] Elixir 1.18 or higher
- [ ] Erlang/OTP 27 or higher
- [ ] Disk space: 2x current database size
- [ ] RAM: 8GB minimum (16GB recommended for large datasets)

### 2.2 Backup Current Database

**CRITICAL: Complete this step before proceeding.**

```bash
# Stop the application (optional - can run live if using snapshot)
# systemctl stop triple_store_app

# Create backup directory
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="/backups/pre_migration_$TIMESTAMP"
mkdir -p "$BACKUP_DIR"

# Copy database files
cp -r /path/to/triple_store/* "$BACKUP_DIR/"

# Verify backup
ls -la "$BACKUP_DIR"

# Note: If application is stopped, restart it
# systemctl start triple_store_app
```

- [ ] Backup completed
- [ ] Backup verified
- [ ] Backup location recorded: _____________________

### 2.3 Dependency Check

```bash
# Navigate to project directory
cd /path/to/triple_store

# Fetch and compile dependencies
mix deps.get
mix compile

# Verify migration module exists
ls lib/triple_store/migration.ex
```

- [ ] Dependencies resolved
- [ ] Code compiles successfully
- [ ] Migration module present

### 2.4 Configuration Check

Verify production configuration includes quad store settings:

```elixir
# config/prod.exs should include:
config :triple_store,
  schema: :quad,  # Ensure quad schema is configured
  migration_source: "/path/to/triple_store",
  migration_target: "/path/to/quad_store"
```

- [ ] Configuration validated
- [ ] Paths are writable
- [ ] Schema set to :quad

---

## 3. Migration Procedure

### 3.1 Preparation Phase (Day Before)

1. **Announce maintenance window**
   ```
   Send notification to stakeholders:
   - Date/Time: _____________________
   - Expected duration: ___ hours
   - Impact: Application read-only during migration
   ```

2. **Final backup verification**
   - [ ] Backup accessible
   - [ ] Backup integrity verified

3. **Pre-migration checklist review**
   - [ ] All items from pre-production checklist completed
   - [ ] Team assembled and available
   - [ ] Communication channels established

### 3.2 Migration Execution

#### Step 1: Dry-Run Migration (Optional but Recommended)

```bash
# Run migration in dry-run mode
mix triple_store.migrate \
  --source /path/to/triple_store \
  --target /path/to/quad_store_test \
  --dry-run \
  --verbose
```

Expected output:
```
[INFO] Starting dry-run migration...
[INFO] Source database: /path/to/triple_store
[INFO] Triple count: 1,234,567
[INFO] Estimated time: 45 minutes
[INFO] Dry-run complete - no changes made
```

- [ ] Dry-run completed successfully
- [ ] Estimated time recorded: _____________________

#### Step 2: Set Application to Read-Only Mode

```bash
# Enable read-only mode (prevents writes during migration)
# This depends on your application architecture

# Option 1: Configuration change
# config/prod.exs: set read_only: true

# Option 2: API endpoint
# curl -X POST http://localhost:4000/admin/read_only

# Option 3: Database flag
# mix triple_store.mode --read-only
```

- [ ] Application in read-only mode
- [ ] Verified no writes occurring

#### Step 3: Create Target Directory

```bash
# Create quad store directory
TARGET_DIR="/path/to/quad_store"
mkdir -p "$TARGET_DIR"

# Verify permissions
ls -ld "$TARGET_DIR"
```

- [ ] Target directory created
- [ ] Permissions verified

#### Step 4: Execute Migration

```bash
# Run the actual migration
mix triple_store.migrate \
  --source /path/to/triple_store \
  --target /path/to/quad_store \
  --batch-size 10000 \
  --verbose \
  --log-file migration.log
```

Monitor the migration progress:

```bash
# In another terminal, monitor progress
tail -f migration.log

# Expected output format:
# [INFO] Migrating triples... (10% complete)
# [INFO] Migrating triples... (20% complete)
# ...
# [INFO] Migration complete!
# [INFO] Total triples migrated: 1,234,567
# [INFO] Total time: 47 minutes 32 seconds
```

- [ ] Migration started
- [ ] Migration in progress (monitor logs)
- [ ] Migration completed

#### Step 5: Validate Migration

```bash
# Run validation
mix triple_store.migrate.validate \
  --source /path/to/triple_store \
  --target /path/to/quad_store
```

Expected validation checks:
```
[INFO] Validating migration...
[OK] Triple count matches: 1,234,567
[OK] Quad count matches: 1,234,567
[OK] All data in default graph (ID 0)
[OK] Dictionary integrity verified
[OK] Index integrity verified (4 indices present)
[SUCCESS] Migration validation passed
```

- [ ] Validation passed
- [ ] All checks successful

### 3.3 Cutover Phase

#### Step 6: Update Application Configuration

```elixir
# config/prod.exs - update database path
config :triple_store,
  db_path: "/path/to/quad_store",  # Changed from triple_store
  schema: :quad
```

#### Step 7: Restart Application

```bash
# Restart with new configuration
# systemctl restart triple_store_app

# Or for releases
./bin/triple_store restart

# Verify startup
./bin/triple_store ping
```

- [ ] Application restarted
- [ ] Application responding

#### Step 8: Smoke Tests

```bash
# Basic smoke tests
curl -X POST http://localhost:4000/api/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * WHERE { ?s ?p ?o } LIMIT 10"}'

# Expected: 200 OK with results
```

- [ ] Basic query successful
- [ ] Data visible in application
- [ ] No errors in application logs

#### Step 9: Disable Read-Only Mode

```bash
# Re-enable writes
curl -X POST http://localhost:4000/admin/read_write
```

- [ ] Write mode enabled
- [ ] Test write successful

---

## 4. Post-Migration Validation

### 4.1 Data Validation

```bash
# Run comprehensive validation
mix triple_store.validate --full
```

Checks to perform:
- [ ] Triple count matches original
- [ ] Sample queries return same results
- [ ] Graph operations work (if applicable)
- [ ] Dictionary integrity OK

### 4.2 Performance Validation

| Metric | Baseline | Target | Actual | Status |
|--------|----------|--------|--------|--------|
| Query latency (p50) | ___ ms | <= baseline | ___ ms | [ ] |
| Query latency (p95) | ___ ms | <= baseline | ___ ms | [ ] |
| Write throughput | ___ ops/s | >= baseline | ___ ops/s | [ ] |

### 4.3 Application Validation

- [ ] All application features working
- [ ] No increase in error rate
- [ ] User acceptance testing complete
- [ ] Stakeholder sign-off obtained

### 4.4 Monitoring Verification

- [ ] Metrics being collected
- [ ] Dashboards showing quad store data
- [ ] No critical alerts firing
- [ ] Health checks passing

---

## 5. Rollback Procedures

### When to Rollback

Initiate rollback if ANY of the following occur:

- [ ] Data validation fails
- [ ] Application critical errors
- [ ] Performance degradation > 2x baseline
- [ ] Application unable to start
- [ ] Data corruption detected

### Rollback Steps

```bash
# 1. Stop the application
systemctl stop triple_store_app

# 2. Restore original database
rm -rf /path/to/quad_store
cp -r /backups/pre_migration_<TIMESTAMP>/* /path/to/triple_store/

# 3. Revert configuration
# Edit config/prod.exs - change db_path back to triple_store
# Change schema back to :triple (if needed)

# 4. Restart application
systemctl start triple_store_app

# 5. Verify
curl http://localhost:4000/health
```

- [ ] Application stopped
- [ ] Database restored
- [ ] Configuration reverted
- [ ] Application restarted
- [ ] Application verified

### Post-Rollback Actions

1. Investigate failure cause
2. Document lessons learned
3. Schedule retry migration
4. Notify stakeholders of rollback

---

## 6. Emergency Contacts

| Role | Name | Phone | Email |
|------|------|-------|-------|
| Migration Lead | ___ | ___ | ___ |
| Tech Lead | ___ | ___ | ___ |
| DBA | ___ | ___ | ___ |
| DevOps Engineer | ___ | ___ | ___ |
| Engineering Manager | ___ | ___ | ___ |

### Escalation Path

1. **Issue occurs** → Migration Lead assesses
2. **Cannot resolve** → Escalate to Tech Lead
3. **Critical issue** → Escalate to Engineering Manager
4. **Rollback decision** → Engineering Manager approval

---

## 7. Estimated Timelines

### By Data Size

| Data Size | Copy Time | Validation Time | Total Time |
|-----------|-----------|-----------------|------------|
| < 1M triples | 15 min | 5 min | 20 min |
| 1M - 10M triples | 30 min | 10 min | 40 min |
| 10M - 100M triples | 2 hours | 30 min | 2.5 hours |
| > 100M triples | 4+ hours | 1 hour | 5+ hours |

### Migration Timeline (Example: 10M triples)

| Time | Activity | Duration |
|------|----------|----------|
| T-1 day | Preparation, dry-run | 1 hour |
| T-30 min | Announce maintenance, final checks | 30 min |
| T-0 | Set read-only, start migration | 5 min |
| T+5 min | Migration running | 35 min |
| T+40 min | Validation | 10 min |
| T+50 min | Cutover, restart | 10 min |
| T+60 min | Smoke tests, monitoring | 20 min |
| T+80 min | Complete, exit maintenance | - |

**Total downtime: ~80 minutes**

---

## 8. Troubleshooting

### Issue: Migration fails to start

**Symptoms:** Error message about missing source or unable to open database

**Solutions:**
1. Verify source path is correct
2. Check file permissions
3. Ensure source database is not corrupted
4. Check RocksDB library version compatibility

```bash
# Verify source database
ls -la /path/to/triple_store

# Check RocksDB
rocksdb_version
```

### Issue: Migration hangs or is very slow

**Symptoms:** No progress updates for >10 minutes

**Solutions:**
1. Check system resources (CPU, memory, disk I/O)
2. Reduce batch size: `--batch-size 5000`
3. Check for disk space issues
4. Monitor RocksDB compaction

```bash
# Monitor progress
tail -f migration.log

# Check system resources
top
iostat -x 5
```

### Issue: Validation fails

**Symptoms:** Validation reports count mismatch or corruption

**Solutions:**
1. Do NOT proceed with cutover
2. Check migration logs for errors
3. Verify source database wasn't modified during migration
4. Consider re-running migration

### Issue: Application won't start after cutover

**Symptoms:** Application crashes or fails to connect

**Solutions:**
1. Check configuration file syntax
2. Verify database path is correct
3. Check file permissions
4. Review application logs

```bash
# Check logs
journalctl -u triple_store_app -f

# Or for releases
./bin/triple_store logs
```

### Issue: Performance degradation after migration

**Symptoms:** Queries slower than baseline

**Solutions:**
1. Wait for RocksDB compaction to complete (may take 30+ min)
2. Check query plans with EXPLAIN
3. Verify statistics are updated
4. Consider tuning RocksDB settings

---

## Appendix: Migration Command Reference

### Full Migration Command

```bash
mix triple_store.migrate \
  --source /path/to/triple_store \
  --target /path/to/quad_store \
  --batch-size 10000 \
  --workers 4 \
  --verbose \
  --log-file migration.log \
  --stats-file migration_stats.json
```

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `--source` | Path to triple store | Required |
| `--target` | Path to quad store (will be created) | Required |
| `--batch-size` | Number of triples per batch | 10000 |
| `--workers` | Number of parallel workers | 4 |
| `--dry-run` | Validate without copying | false |
| `--verbose` | Detailed logging | false |
| `--log-file` | Log file path | stdout |
| `--stats-file` | Statistics output | - |

### Validation Command

```bash
mix triple_store.migrate.validate \
  --source /path/to/triple_store \
  --target /path/to/quad_store \
  --full
```

---

## Runbook Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2026-01-20 | Initial version |
