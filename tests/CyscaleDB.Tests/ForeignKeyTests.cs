using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Tests;

/// <summary>
/// Tests for foreign key constraint enforcement.
/// </summary>
public class ForeignKeyTests : IDisposable
{
    private readonly string _testDir;
    private readonly Catalog _catalog;
    private readonly Executor _executor;
    private readonly ForeignKeyManager _fkManager;

    public ForeignKeyTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"cyscaledb_fk_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);

        _catalog = new Catalog(_testDir);
        _catalog.Initialize();

        // Create test database
        _catalog.CreateDatabase("testdb");

        _fkManager = new ForeignKeyManager();
        _executor = new Executor(_catalog, null, _fkManager, "testdb");
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_testDir))
        {
            Directory.Delete(_testDir, true);
        }
    }

    #region Setup Helper Methods

    private void CreateParentChildTables()
    {
        // Create parent table
        _executor.Execute(@"
            CREATE TABLE parent (
                id INT PRIMARY KEY,
                name VARCHAR(100)
            )");

        // Create child table with FK
        _executor.Execute(@"
            CREATE TABLE child (
                id INT PRIMARY KEY,
                parent_id INT,
                value VARCHAR(100)
            )");

        // Register the foreign key constraint
        _fkManager.AddForeignKey(
            constraintName: "fk_child_parent",
            databaseName: "testdb",
            tableName: "child",
            columns: new[] { "parent_id" },
            referencedDatabaseName: "testdb",
            referencedTableName: "parent",
            referencedColumns: new[] { "id" },
            onDelete: ForeignKeyAction.Restrict,
            onUpdate: ForeignKeyAction.Restrict);
    }

    #endregion

    #region INSERT Tests

    [Fact]
    public void Insert_WithValidForeignKey_Succeeds()
    {
        CreateParentChildTables();

        // Insert parent row first
        _executor.Execute("INSERT INTO parent (id, name) VALUES (1, 'Parent 1')");

        // Insert child with valid FK - should succeed
        var result = _executor.Execute("INSERT INTO child (id, parent_id, value) VALUES (1, 1, 'Child 1')");

        Assert.Equal(1, result.AffectedRows);

        // Verify the child was inserted
        var select = _executor.Execute("SELECT * FROM child WHERE id = 1");
        Assert.True(select.ResultSet != null);
        Assert.Equal(1, select.ResultSet.Rows.Count);
    }

    [Fact]
    public void Insert_WithInvalidForeignKey_ThrowsConstraintViolation()
    {
        CreateParentChildTables();

        // Insert parent row
        _executor.Execute("INSERT INTO parent (id, name) VALUES (1, 'Parent 1')");

        // Try to insert child with non-existent parent - should fail
        var ex = Assert.Throws<ConstraintViolationException>(() =>
            _executor.Execute("INSERT INTO child (id, parent_id, value) VALUES (1, 999, 'Child 1')"));

        Assert.Contains("fk_child_parent", ex.Message);
    }

    [Fact]
    public void Insert_WithNullForeignKey_Succeeds()
    {
        CreateParentChildTables();

        // Insert parent row
        _executor.Execute("INSERT INTO parent (id, name) VALUES (1, 'Parent 1')");

        // Insert child with NULL FK - should succeed (NULL doesn't reference anything)
        var result = _executor.Execute("INSERT INTO child (id, parent_id, value) VALUES (1, NULL, 'Child 1')");

        Assert.Equal(1, result.AffectedRows);
    }

    #endregion

    #region DELETE Tests (RESTRICT)

    [Fact]
    public void Delete_ParentWithNoChildren_Succeeds()
    {
        CreateParentChildTables();

        // Insert parent row only
        _executor.Execute("INSERT INTO parent (id, name) VALUES (1, 'Parent 1')");

        // Delete parent - should succeed (no children reference it)
        var result = _executor.Execute("DELETE FROM parent WHERE id = 1");

        Assert.Equal(1, result.AffectedRows);
    }

    [Fact]
    public void Delete_ParentWithChildren_Restrict_ThrowsConstraintViolation()
    {
        CreateParentChildTables();

        // Insert parent and child
        _executor.Execute("INSERT INTO parent (id, name) VALUES (1, 'Parent 1')");
        _executor.Execute("INSERT INTO child (id, parent_id, value) VALUES (1, 1, 'Child 1')");

        // Try to delete parent - should fail due to RESTRICT
        var ex = Assert.Throws<ConstraintViolationException>(() =>
            _executor.Execute("DELETE FROM parent WHERE id = 1"));

        Assert.Contains("fk_child_parent", ex.Message);
    }

    [Fact]
    public void Delete_Child_Succeeds()
    {
        CreateParentChildTables();

        // Insert parent and child
        _executor.Execute("INSERT INTO parent (id, name) VALUES (1, 'Parent 1')");
        _executor.Execute("INSERT INTO child (id, parent_id, value) VALUES (1, 1, 'Child 1')");

        // Delete child - should succeed
        var result = _executor.Execute("DELETE FROM child WHERE id = 1");

        Assert.Equal(1, result.AffectedRows);

        // Now can delete parent
        result = _executor.Execute("DELETE FROM parent WHERE id = 1");
        Assert.Equal(1, result.AffectedRows);
    }

    [Fact]
    public void Delete_ParentWithMultipleChildren_Restrict_ThrowsConstraintViolation()
    {
        CreateParentChildTables();

        // Insert parent and multiple children
        _executor.Execute("INSERT INTO parent (id, name) VALUES (1, 'Parent 1')");
        _executor.Execute("INSERT INTO child (id, parent_id, value) VALUES (1, 1, 'Child 1')");
        _executor.Execute("INSERT INTO child (id, parent_id, value) VALUES (2, 1, 'Child 2')");

        // Try to delete parent - should fail
        Assert.Throws<ConstraintViolationException>(() =>
            _executor.Execute("DELETE FROM parent WHERE id = 1"));
    }

    #endregion

    #region UPDATE Tests (RESTRICT)

    [Fact]
    public void Update_ParentPrimaryKey_WithNoChildren_Succeeds()
    {
        CreateParentChildTables();

        // Insert parent only
        _executor.Execute("INSERT INTO parent (id, name) VALUES (1, 'Parent 1')");

        // Update parent PK - should succeed (no children reference it)
        var result = _executor.Execute("UPDATE parent SET id = 2 WHERE id = 1");

        Assert.Equal(1, result.AffectedRows);
    }

    [Fact]
    public void Update_ParentPrimaryKey_WithChildren_Restrict_ThrowsConstraintViolation()
    {
        CreateParentChildTables();

        // Insert parent and child
        _executor.Execute("INSERT INTO parent (id, name) VALUES (1, 'Parent 1')");
        _executor.Execute("INSERT INTO child (id, parent_id, value) VALUES (1, 1, 'Child 1')");

        // Try to update parent PK - should fail due to RESTRICT
        var ex = Assert.Throws<ConstraintViolationException>(() =>
            _executor.Execute("UPDATE parent SET id = 2 WHERE id = 1"));

        Assert.Contains("fk_child_parent", ex.Message);
    }

    [Fact]
    public void Update_ChildForeignKey_ToValidValue_Succeeds()
    {
        CreateParentChildTables();

        // Insert two parents and a child
        _executor.Execute("INSERT INTO parent (id, name) VALUES (1, 'Parent 1')");
        _executor.Execute("INSERT INTO parent (id, name) VALUES (2, 'Parent 2')");
        _executor.Execute("INSERT INTO child (id, parent_id, value) VALUES (1, 1, 'Child 1')");

        // Update child FK to point to different parent - should succeed
        var result = _executor.Execute("UPDATE child SET parent_id = 2 WHERE id = 1");

        Assert.Equal(1, result.AffectedRows);
    }

    [Fact]
    public void Update_ChildForeignKey_ToInvalidValue_ThrowsConstraintViolation()
    {
        CreateParentChildTables();

        // Insert parent and child
        _executor.Execute("INSERT INTO parent (id, name) VALUES (1, 'Parent 1')");
        _executor.Execute("INSERT INTO child (id, parent_id, value) VALUES (1, 1, 'Child 1')");

        // Try to update child FK to non-existent parent - should fail
        var ex = Assert.Throws<ConstraintViolationException>(() =>
            _executor.Execute("UPDATE child SET parent_id = 999 WHERE id = 1"));

        Assert.Contains("fk_child_parent", ex.Message);
    }

    [Fact]
    public void Update_ChildForeignKey_ToNull_Succeeds()
    {
        CreateParentChildTables();

        // Insert parent and child
        _executor.Execute("INSERT INTO parent (id, name) VALUES (1, 'Parent 1')");
        _executor.Execute("INSERT INTO child (id, parent_id, value) VALUES (1, 1, 'Child 1')");

        // Update child FK to NULL - should succeed
        var result = _executor.Execute("UPDATE child SET parent_id = NULL WHERE id = 1");

        Assert.Equal(1, result.AffectedRows);
    }

    [Fact]
    public void Update_ParentNonKeyColumn_WithChildren_Succeeds()
    {
        CreateParentChildTables();

        // Insert parent and child
        _executor.Execute("INSERT INTO parent (id, name) VALUES (1, 'Parent 1')");
        _executor.Execute("INSERT INTO child (id, parent_id, value) VALUES (1, 1, 'Child 1')");

        // Update parent non-key column - should succeed
        var result = _executor.Execute("UPDATE parent SET name = 'Updated Parent' WHERE id = 1");

        Assert.Equal(1, result.AffectedRows);
    }

    #endregion

    #region CASCADE DELETE Tests

    private void CreateParentChildTablesWithCascade()
    {
        // Create parent table
        _executor.Execute(@"
            CREATE TABLE parent (
                id INT PRIMARY KEY,
                name VARCHAR(100)
            )");

        // Create child table with FK CASCADE DELETE
        _executor.Execute(@"
            CREATE TABLE child (
                id INT PRIMARY KEY,
                parent_id INT,
                value VARCHAR(100)
            )");

        // Register the foreign key constraint with CASCADE DELETE
        _fkManager.AddForeignKey(
            constraintName: "fk_child_parent_cascade",
            databaseName: "testdb",
            tableName: "child",
            columns: new[] { "parent_id" },
            referencedDatabaseName: "testdb",
            referencedTableName: "parent",
            referencedColumns: new[] { "id" },
            onDelete: ForeignKeyAction.Cascade,
            onUpdate: ForeignKeyAction.Restrict);
    }

    [Fact]
    public void Delete_ParentWithChildren_Cascade_DeletesChildren()
    {
        CreateParentChildTablesWithCascade();

        // Insert parent and children
        _executor.Execute("INSERT INTO parent (id, name) VALUES (1, 'Parent 1')");
        _executor.Execute("INSERT INTO child (id, parent_id, value) VALUES (1, 1, 'Child 1')");
        _executor.Execute("INSERT INTO child (id, parent_id, value) VALUES (2, 1, 'Child 2')");

        // Verify children exist
        var beforeDelete = _executor.Execute("SELECT COUNT(*) FROM child WHERE parent_id = 1");
        Assert.Equal(2L, beforeDelete.ResultSet!.Rows[0][0].AsBigInt());

        // Delete parent - should cascade to children
        var result = _executor.Execute("DELETE FROM parent WHERE id = 1");
        Assert.Equal(1, result.AffectedRows);

        // Verify children are deleted
        var afterDelete = _executor.Execute("SELECT COUNT(*) FROM child WHERE parent_id = 1");
        Assert.Equal(0L, afterDelete.ResultSet!.Rows[0][0].AsBigInt());
    }

    [Fact]
    public void Delete_ParentWithChildren_Cascade_LeavesOtherChildren()
    {
        CreateParentChildTablesWithCascade();

        // Insert two parents with children
        _executor.Execute("INSERT INTO parent (id, name) VALUES (1, 'Parent 1')");
        _executor.Execute("INSERT INTO parent (id, name) VALUES (2, 'Parent 2')");
        _executor.Execute("INSERT INTO child (id, parent_id, value) VALUES (1, 1, 'Child of P1')");
        _executor.Execute("INSERT INTO child (id, parent_id, value) VALUES (2, 2, 'Child of P2')");

        // Delete parent 1 - should only cascade to its children
        _executor.Execute("DELETE FROM parent WHERE id = 1");

        // Verify parent 2's child still exists
        var remainingChildren = _executor.Execute("SELECT COUNT(*) FROM child WHERE parent_id = 2");
        Assert.Equal(1L, remainingChildren.ResultSet!.Rows[0][0].AsBigInt());
    }

    [Fact]
    public void Delete_ParentWithNoChildren_Cascade_Succeeds()
    {
        CreateParentChildTablesWithCascade();

        // Insert parent without children
        _executor.Execute("INSERT INTO parent (id, name) VALUES (1, 'Parent 1')");

        // Delete parent - should succeed
        var result = _executor.Execute("DELETE FROM parent WHERE id = 1");
        Assert.Equal(1, result.AffectedRows);
    }

    [Fact]
    public void Delete_CascadeMultipleLevels()
    {
        // Create grandparent -> parent -> child hierarchy
        _executor.Execute("CREATE TABLE grandparent (id INT PRIMARY KEY, name VARCHAR(100))");
        _executor.Execute("CREATE TABLE parent2 (id INT PRIMARY KEY, gp_id INT, name VARCHAR(100))");
        _executor.Execute("CREATE TABLE child2 (id INT PRIMARY KEY, p_id INT, value VARCHAR(100))");

        // FK from parent2 -> grandparent with CASCADE
        _fkManager.AddForeignKey(
            constraintName: "fk_parent_grandparent",
            databaseName: "testdb",
            tableName: "parent2",
            columns: new[] { "gp_id" },
            referencedDatabaseName: "testdb",
            referencedTableName: "grandparent",
            referencedColumns: new[] { "id" },
            onDelete: ForeignKeyAction.Cascade,
            onUpdate: ForeignKeyAction.Restrict);

        // FK from child2 -> parent2 with CASCADE
        _fkManager.AddForeignKey(
            constraintName: "fk_child_parent2",
            databaseName: "testdb",
            tableName: "child2",
            columns: new[] { "p_id" },
            referencedDatabaseName: "testdb",
            referencedTableName: "parent2",
            referencedColumns: new[] { "id" },
            onDelete: ForeignKeyAction.Cascade,
            onUpdate: ForeignKeyAction.Restrict);

        // Insert hierarchy
        _executor.Execute("INSERT INTO grandparent (id, name) VALUES (1, 'Grandparent')");
        _executor.Execute("INSERT INTO parent2 (id, gp_id, name) VALUES (1, 1, 'Parent')");
        _executor.Execute("INSERT INTO child2 (id, p_id, value) VALUES (1, 1, 'Child')");

        // Delete grandparent - should cascade all the way down
        var result = _executor.Execute("DELETE FROM grandparent WHERE id = 1");
        Assert.Equal(1, result.AffectedRows);

        // Verify parent2 is deleted
        var parentCount = _executor.Execute("SELECT COUNT(*) FROM parent2");
        Assert.Equal(0L, parentCount.ResultSet!.Rows[0][0].AsBigInt());

        // Verify child2 is deleted
        var childCount = _executor.Execute("SELECT COUNT(*) FROM child2");
        Assert.Equal(0L, childCount.ResultSet!.Rows[0][0].AsBigInt());
    }

    #endregion

    #region CASCADE UPDATE Tests

    private void CreateParentChildTablesWithCascadeUpdate()
    {
        // Create parent table
        _executor.Execute(@"
            CREATE TABLE parent (
                id INT PRIMARY KEY,
                name VARCHAR(100)
            )");

        // Create child table with FK CASCADE UPDATE
        _executor.Execute(@"
            CREATE TABLE child (
                id INT PRIMARY KEY,
                parent_id INT,
                value VARCHAR(100)
            )");

        // Register the foreign key constraint with CASCADE UPDATE
        _fkManager.AddForeignKey(
            constraintName: "fk_child_parent_cascade_update",
            databaseName: "testdb",
            tableName: "child",
            columns: new[] { "parent_id" },
            referencedDatabaseName: "testdb",
            referencedTableName: "parent",
            referencedColumns: new[] { "id" },
            onDelete: ForeignKeyAction.Restrict,
            onUpdate: ForeignKeyAction.Cascade);
    }

    [Fact]
    public void Update_ParentPrimaryKey_Cascade_UpdatesChildren()
    {
        CreateParentChildTablesWithCascadeUpdate();

        // Insert parent and children
        _executor.Execute("INSERT INTO parent (id, name) VALUES (1, 'Parent 1')");
        _executor.Execute("INSERT INTO child (id, parent_id, value) VALUES (1, 1, 'Child 1')");
        _executor.Execute("INSERT INTO child (id, parent_id, value) VALUES (2, 1, 'Child 2')");

        // Verify children have parent_id = 1
        var beforeUpdate = _executor.Execute("SELECT parent_id FROM child WHERE id = 1");
        Assert.Equal(1, beforeUpdate.ResultSet!.Rows[0][0].AsInt());

        // Update parent PK - should cascade to children
        var result = _executor.Execute("UPDATE parent SET id = 100 WHERE id = 1");
        Assert.Equal(1, result.AffectedRows);

        // Verify children now have parent_id = 100
        var afterUpdate = _executor.Execute("SELECT parent_id FROM child WHERE id = 1");
        Assert.Equal(100, afterUpdate.ResultSet!.Rows[0][0].AsInt());

        // Verify all children were updated
        var childCount = _executor.Execute("SELECT COUNT(*) FROM child WHERE parent_id = 100");
        Assert.Equal(2L, childCount.ResultSet!.Rows[0][0].AsBigInt());
    }

    [Fact]
    public void Update_ParentPrimaryKey_Cascade_LeavesOtherChildren()
    {
        CreateParentChildTablesWithCascadeUpdate();

        // Insert two parents with children
        _executor.Execute("INSERT INTO parent (id, name) VALUES (1, 'Parent 1')");
        _executor.Execute("INSERT INTO parent (id, name) VALUES (2, 'Parent 2')");
        _executor.Execute("INSERT INTO child (id, parent_id, value) VALUES (1, 1, 'Child of P1')");
        _executor.Execute("INSERT INTO child (id, parent_id, value) VALUES (2, 2, 'Child of P2')");

        // Update parent 1's PK - should only cascade to its children
        _executor.Execute("UPDATE parent SET id = 100 WHERE id = 1");

        // Verify parent 2's child still has parent_id = 2
        var p2Child = _executor.Execute("SELECT parent_id FROM child WHERE id = 2");
        Assert.Equal(2, p2Child.ResultSet!.Rows[0][0].AsInt());
    }

    [Fact]
    public void Update_ParentPrimaryKey_WithNoChildren_Cascade_Succeeds()
    {
        CreateParentChildTablesWithCascadeUpdate();

        // Insert parent without children
        _executor.Execute("INSERT INTO parent (id, name) VALUES (1, 'Parent 1')");

        // Update parent PK - should succeed
        var result = _executor.Execute("UPDATE parent SET id = 100 WHERE id = 1");
        Assert.Equal(1, result.AffectedRows);
    }

    [Fact]
    public void Update_CascadeMultipleLevels()
    {
        // Create grandparent -> parent -> child hierarchy
        _executor.Execute("CREATE TABLE gp (id INT PRIMARY KEY, name VARCHAR(100))");
        _executor.Execute("CREATE TABLE parent_u (id INT PRIMARY KEY, gp_id INT, name VARCHAR(100))");
        _executor.Execute("CREATE TABLE child_u (id INT PRIMARY KEY, p_id INT, value VARCHAR(100))");

        // FK from parent_u -> gp with CASCADE UPDATE
        _fkManager.AddForeignKey(
            constraintName: "fk_parent_gp_update",
            databaseName: "testdb",
            tableName: "parent_u",
            columns: new[] { "gp_id" },
            referencedDatabaseName: "testdb",
            referencedTableName: "gp",
            referencedColumns: new[] { "id" },
            onDelete: ForeignKeyAction.Restrict,
            onUpdate: ForeignKeyAction.Cascade);

        // FK from child_u -> parent_u with CASCADE UPDATE
        _fkManager.AddForeignKey(
            constraintName: "fk_child_parent_u_update",
            databaseName: "testdb",
            tableName: "child_u",
            columns: new[] { "p_id" },
            referencedDatabaseName: "testdb",
            referencedTableName: "parent_u",
            referencedColumns: new[] { "id" },
            onDelete: ForeignKeyAction.Restrict,
            onUpdate: ForeignKeyAction.Cascade);

        // Insert hierarchy
        _executor.Execute("INSERT INTO gp (id, name) VALUES (1, 'Grandparent')");
        _executor.Execute("INSERT INTO parent_u (id, gp_id, name) VALUES (1, 1, 'Parent')");
        _executor.Execute("INSERT INTO child_u (id, p_id, value) VALUES (1, 1, 'Child')");

        // Update grandparent PK - should cascade to parent (gp_id)
        // Note: This doesn't cascade all the way down because we're updating gp.id, 
        // which affects parent_u.gp_id, not parent_u.id
        var result = _executor.Execute("UPDATE gp SET id = 100 WHERE id = 1");
        Assert.Equal(1, result.AffectedRows);

        // Verify parent_u.gp_id was updated
        var parentCheck = _executor.Execute("SELECT gp_id FROM parent_u WHERE id = 1");
        Assert.Equal(100, parentCheck.ResultSet!.Rows[0][0].AsInt());

        // Verify child_u.p_id is still 1 (unchanged because parent_u.id didn't change)
        var childCheck = _executor.Execute("SELECT p_id FROM child_u WHERE id = 1");
        Assert.Equal(1, childCheck.ResultSet!.Rows[0][0].AsInt());
    }

    #endregion

    #region SET NULL Tests

    private void CreateParentChildTablesWithSetNull()
    {
        // Create parent table
        _executor.Execute(@"
            CREATE TABLE parent (
                id INT PRIMARY KEY,
                name VARCHAR(100)
            )");

        // Create child table with FK SET NULL on DELETE and UPDATE
        _executor.Execute(@"
            CREATE TABLE child (
                id INT PRIMARY KEY,
                parent_id INT,
                value VARCHAR(100)
            )");

        // Register the foreign key constraint with SET NULL
        _fkManager.AddForeignKey(
            constraintName: "fk_child_parent_setnull",
            databaseName: "testdb",
            tableName: "child",
            columns: new[] { "parent_id" },
            referencedDatabaseName: "testdb",
            referencedTableName: "parent",
            referencedColumns: new[] { "id" },
            onDelete: ForeignKeyAction.SetNull,
            onUpdate: ForeignKeyAction.SetNull);
    }

    [Fact]
    public void Delete_ParentWithChildren_SetNull_SetsChildForeignKeyToNull()
    {
        CreateParentChildTablesWithSetNull();

        // Insert parent and children
        _executor.Execute("INSERT INTO parent (id, name) VALUES (1, 'Parent 1')");
        _executor.Execute("INSERT INTO child (id, parent_id, value) VALUES (1, 1, 'Child 1')");
        _executor.Execute("INSERT INTO child (id, parent_id, value) VALUES (2, 1, 'Child 2')");

        // Verify children have parent_id = 1 (not NULL)
        var beforeDelete = _executor.Execute("SELECT parent_id FROM child WHERE id = 1");
        Assert.False(beforeDelete.ResultSet!.Rows[0][0].IsNull);
        Assert.Equal(1, beforeDelete.ResultSet!.Rows[0][0].AsInt());

        // Delete parent - should set children's parent_id to NULL
        var result = _executor.Execute("DELETE FROM parent WHERE id = 1");
        Assert.Equal(1, result.AffectedRows);

        // Verify children now have parent_id = NULL
        var afterDelete = _executor.Execute("SELECT parent_id FROM child WHERE id = 1");
        Assert.True(afterDelete.ResultSet!.Rows[0][0].IsNull);

        // Verify all children have NULL parent_id
        var childNullCount = _executor.Execute("SELECT COUNT(*) FROM child WHERE parent_id IS NULL");
        Assert.Equal(2L, childNullCount.ResultSet!.Rows[0][0].AsBigInt());
    }

    [Fact]
    public void Update_ParentPrimaryKey_SetNull_SetsChildForeignKeyToNull()
    {
        CreateParentChildTablesWithSetNull();

        // Insert parent and children
        _executor.Execute("INSERT INTO parent (id, name) VALUES (1, 'Parent 1')");
        _executor.Execute("INSERT INTO child (id, parent_id, value) VALUES (1, 1, 'Child 1')");

        // Verify child has parent_id = 1
        var beforeUpdate = _executor.Execute("SELECT parent_id FROM child WHERE id = 1");
        Assert.Equal(1, beforeUpdate.ResultSet!.Rows[0][0].AsInt());

        // Update parent PK - should set child's parent_id to NULL
        var result = _executor.Execute("UPDATE parent SET id = 100 WHERE id = 1");
        Assert.Equal(1, result.AffectedRows);

        // Verify child now has parent_id = NULL
        var afterUpdate = _executor.Execute("SELECT parent_id FROM child WHERE id = 1");
        Assert.True(afterUpdate.ResultSet!.Rows[0][0].IsNull);
    }

    [Fact]
    public void Delete_SetNull_LeavesOtherChildrenIntact()
    {
        CreateParentChildTablesWithSetNull();

        // Insert two parents with children
        _executor.Execute("INSERT INTO parent (id, name) VALUES (1, 'Parent 1')");
        _executor.Execute("INSERT INTO parent (id, name) VALUES (2, 'Parent 2')");
        _executor.Execute("INSERT INTO child (id, parent_id, value) VALUES (1, 1, 'Child of P1')");
        _executor.Execute("INSERT INTO child (id, parent_id, value) VALUES (2, 2, 'Child of P2')");

        // Delete parent 1 - should only set its child's FK to NULL
        _executor.Execute("DELETE FROM parent WHERE id = 1");

        // Verify parent 1's child has NULL FK
        var p1Child = _executor.Execute("SELECT parent_id FROM child WHERE id = 1");
        Assert.True(p1Child.ResultSet!.Rows[0][0].IsNull);

        // Verify parent 2's child still has parent_id = 2
        var p2Child = _executor.Execute("SELECT parent_id FROM child WHERE id = 2");
        Assert.False(p2Child.ResultSet!.Rows[0][0].IsNull);
        Assert.Equal(2, p2Child.ResultSet!.Rows[0][0].AsInt());
    }

    #endregion

    #region ForeignKeyManager Tests

    [Fact]
    public void ForeignKeyManager_AddAndGetForeignKey()
    {
        var manager = new ForeignKeyManager();

        var fk = manager.AddForeignKey(
            constraintName: "test_fk",
            databaseName: "testdb",
            tableName: "orders",
            columns: new[] { "customer_id" },
            referencedDatabaseName: "testdb",
            referencedTableName: "customers",
            referencedColumns: new[] { "id" },
            onDelete: ForeignKeyAction.Restrict,
            onUpdate: ForeignKeyAction.Cascade);

        Assert.NotNull(fk);
        Assert.Equal("test_fk", fk.ConstraintName);
        Assert.Equal(ForeignKeyAction.Restrict, fk.OnDelete);
        Assert.Equal(ForeignKeyAction.Cascade, fk.OnUpdate);

        var retrieved = manager.GetForeignKey("testdb", "test_fk");
        Assert.NotNull(retrieved);
        Assert.Equal(fk.ForeignKeyId, retrieved.ForeignKeyId);
    }

    [Fact]
    public void ForeignKeyManager_GetForeignKeysOnTable()
    {
        var manager = new ForeignKeyManager();

        manager.AddForeignKey("fk1", "db", "child", new[] { "col1" }, "db", "parent1", new[] { "id" });
        manager.AddForeignKey("fk2", "db", "child", new[] { "col2" }, "db", "parent2", new[] { "id" });

        var fks = manager.GetForeignKeysOnTable("db", "child");

        Assert.Equal(2, fks.Count);
    }

    [Fact]
    public void ForeignKeyManager_GetForeignKeysReferencingTable()
    {
        var manager = new ForeignKeyManager();

        manager.AddForeignKey("fk1", "db", "child1", new[] { "parent_id" }, "db", "parent", new[] { "id" });
        manager.AddForeignKey("fk2", "db", "child2", new[] { "parent_id" }, "db", "parent", new[] { "id" });

        var fks = manager.GetForeignKeysReferencingTable("db", "parent");

        Assert.Equal(2, fks.Count);
    }

    [Fact]
    public void ForeignKeyManager_DropForeignKey()
    {
        var manager = new ForeignKeyManager();

        manager.AddForeignKey("test_fk", "db", "child", new[] { "parent_id" }, "db", "parent", new[] { "id" });

        Assert.NotNull(manager.GetForeignKey("db", "test_fk"));

        var dropped = manager.DropForeignKey("db", "test_fk");

        Assert.True(dropped);
        Assert.Null(manager.GetForeignKey("db", "test_fk"));
    }

    [Fact]
    public void ForeignKeyInfo_Serialization()
    {
        var fk = new ForeignKeyInfo(
            foreignKeyId: 1,
            constraintName: "test_fk",
            databaseName: "testdb",
            tableName: "orders",
            columns: new[] { "customer_id", "product_id" },
            referencedDatabaseName: "testdb",
            referencedTableName: "customers",
            referencedColumns: new[] { "id", "product_key" },
            onDelete: ForeignKeyAction.Cascade,
            onUpdate: ForeignKeyAction.SetNull);

        var bytes = fk.Serialize();
        var deserialized = ForeignKeyInfo.Deserialize(bytes);

        Assert.Equal(fk.ForeignKeyId, deserialized.ForeignKeyId);
        Assert.Equal(fk.ConstraintName, deserialized.ConstraintName);
        Assert.Equal(fk.DatabaseName, deserialized.DatabaseName);
        Assert.Equal(fk.TableName, deserialized.TableName);
        Assert.Equal(fk.Columns, deserialized.Columns);
        Assert.Equal(fk.ReferencedDatabaseName, deserialized.ReferencedDatabaseName);
        Assert.Equal(fk.ReferencedTableName, deserialized.ReferencedTableName);
        Assert.Equal(fk.ReferencedColumns, deserialized.ReferencedColumns);
        Assert.Equal(fk.OnDelete, deserialized.OnDelete);
        Assert.Equal(fk.OnUpdate, deserialized.OnUpdate);
    }

    #endregion
}
