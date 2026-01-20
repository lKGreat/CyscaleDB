using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Tests;

public class ViewTests
{
    [Fact]
    public void ViewInfo_Create_WithBasicProperties()
    {
        var view = new ViewInfo(1, "active_users", "testdb",
            "SELECT * FROM users WHERE active = 1");

        Assert.Equal(1, view.ViewId);
        Assert.Equal("active_users", view.ViewName);
        Assert.Equal("testdb", view.DatabaseName);
        Assert.Equal("SELECT * FROM users WHERE active = 1", view.Definition);
        Assert.Null(view.ColumnNames);
    }

    [Fact]
    public void ViewInfo_Create_WithColumnNames()
    {
        var view = new ViewInfo(2, "user_summary", "testdb",
            "SELECT id, name FROM users",
            columnNames: new[] { "user_id", "user_name" });

        Assert.NotNull(view.ColumnNames);
        Assert.Equal(2, view.ColumnNames!.Count);
        Assert.Equal("user_id", view.ColumnNames![0]);
        Assert.Equal("user_name", view.ColumnNames![1]);
    }

    [Fact]
    public void ViewInfo_Serialize_Deserialize()
    {
        var original = new ViewInfo(3, "test_view", "mydb",
            "SELECT a, b FROM t WHERE x > 10",
            columnNames: new[] { "col_a", "col_b" },
            orReplace: true);

        var bytes = original.Serialize();
        var deserialized = ViewInfo.Deserialize(bytes);

        Assert.Equal(original.ViewId, deserialized.ViewId);
        Assert.Equal(original.ViewName, deserialized.ViewName);
        Assert.Equal(original.DatabaseName, deserialized.DatabaseName);
        Assert.Equal(original.Definition, deserialized.Definition);
        Assert.Equal(original.ColumnNames!.Count, deserialized.ColumnNames!.Count);
        Assert.Equal(original.OrReplace, deserialized.OrReplace);
    }

    [Fact]
    public void ViewInfo_Serialize_Deserialize_NoColumnNames()
    {
        var original = new ViewInfo(4, "simple_view", "db", "SELECT * FROM t");

        var bytes = original.Serialize();
        var deserialized = ViewInfo.Deserialize(bytes);

        Assert.Null(deserialized.ColumnNames);
    }

    [Fact]
    public void ViewInfo_Create_ThrowsForEmptyName()
    {
        Assert.Throws<ArgumentException>(() =>
            new ViewInfo(1, "", "db", "SELECT 1"));
    }

    [Fact]
    public void ViewInfo_Create_ThrowsForEmptyDefinition()
    {
        Assert.Throws<ArgumentException>(() =>
            new ViewInfo(1, "view1", "db", ""));
    }

    [Fact]
    public void DatabaseInfo_AddView_And_GetView()
    {
        var db = new DatabaseInfo(1, "testdb", "/data/testdb");
        var view = new ViewInfo(1, "my_view", "testdb", "SELECT 1");

        db.AddView(view);

        Assert.True(db.HasView("my_view"));
        Assert.NotNull(db.GetView("my_view"));
        Assert.Equal(1, db.GetView("my_view")!.ViewId);
    }

    [Fact]
    public void DatabaseInfo_RemoveView()
    {
        var db = new DatabaseInfo(1, "testdb", "/data/testdb");
        var view = new ViewInfo(1, "my_view", "testdb", "SELECT 1");

        db.AddView(view);
        Assert.True(db.HasView("my_view"));

        db.RemoveView("my_view");
        Assert.False(db.HasView("my_view"));
    }

    [Fact]
    public void DatabaseInfo_AddOrReplaceView_ReplacesExisting()
    {
        var db = new DatabaseInfo(1, "testdb", "/data/testdb");

        var view1 = new ViewInfo(1, "my_view", "testdb", "SELECT 1");
        var view2 = new ViewInfo(2, "my_view", "testdb", "SELECT 2");

        db.AddView(view1);
        db.AddOrReplaceView(view2);

        var retrieved = db.GetView("my_view");
        Assert.Equal(2, retrieved!.ViewId);
        Assert.Equal("SELECT 2", retrieved.Definition);
    }

    [Fact]
    public void DatabaseInfo_Views_Property_ReturnsAllViews()
    {
        var db = new DatabaseInfo(1, "testdb", "/data/testdb");

        db.AddView(new ViewInfo(1, "view1", "testdb", "SELECT 1"));
        db.AddView(new ViewInfo(2, "view2", "testdb", "SELECT 2"));

        Assert.Equal(2, db.Views.Count);
    }

    [Fact]
    public void DatabaseInfo_Serialize_WithViews()
    {
        var original = new DatabaseInfo(1, "testdb", "/data/testdb");
        original.AddView(new ViewInfo(1, "v1", "testdb", "SELECT a FROM t"));
        original.AddView(new ViewInfo(2, "v2", "testdb", "SELECT b FROM t"));

        var bytes = original.Serialize();
        var deserialized = DatabaseInfo.Deserialize(bytes);

        Assert.Equal(2, deserialized.Views.Count);
        Assert.True(deserialized.HasView("v1"));
        Assert.True(deserialized.HasView("v2"));
    }
}
