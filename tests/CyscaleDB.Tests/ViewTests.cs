using NUnit.Framework;
using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Tests;

[TestFixture]
public class ViewTests
{
    [Test]
    public void ViewInfo_Create_WithBasicProperties()
    {
        var view = new ViewInfo(1, "active_users", "testdb",
            "SELECT * FROM users WHERE active = 1");

        Assert.Multiple(() =>
        {
            Assert.That(view.ViewId, Is.EqualTo(1));
            Assert.That(view.ViewName, Is.EqualTo("active_users"));
            Assert.That(view.DatabaseName, Is.EqualTo("testdb"));
            Assert.That(view.Definition, Is.EqualTo("SELECT * FROM users WHERE active = 1"));
            Assert.That(view.ColumnNames, Is.Null);
        });
    }

    [Test]
    public void ViewInfo_Create_WithColumnNames()
    {
        var view = new ViewInfo(2, "user_summary", "testdb",
            "SELECT id, name FROM users",
            columnNames: new[] { "user_id", "user_name" });

        Assert.Multiple(() =>
        {
            Assert.That(view.ColumnNames, Is.Not.Null);
            Assert.That(view.ColumnNames!.Count, Is.EqualTo(2));
            Assert.That(view.ColumnNames![0], Is.EqualTo("user_id"));
            Assert.That(view.ColumnNames![1], Is.EqualTo("user_name"));
        });
    }

    [Test]
    public void ViewInfo_Serialize_Deserialize()
    {
        var original = new ViewInfo(3, "test_view", "mydb",
            "SELECT a, b FROM t WHERE x > 10",
            columnNames: new[] { "col_a", "col_b" },
            orReplace: true);

        var bytes = original.Serialize();
        var deserialized = ViewInfo.Deserialize(bytes);

        Assert.Multiple(() =>
        {
            Assert.That(deserialized.ViewId, Is.EqualTo(original.ViewId));
            Assert.That(deserialized.ViewName, Is.EqualTo(original.ViewName));
            Assert.That(deserialized.DatabaseName, Is.EqualTo(original.DatabaseName));
            Assert.That(deserialized.Definition, Is.EqualTo(original.Definition));
            Assert.That(deserialized.ColumnNames!.Count, Is.EqualTo(original.ColumnNames!.Count));
            Assert.That(deserialized.OrReplace, Is.EqualTo(original.OrReplace));
        });
    }

    [Test]
    public void ViewInfo_Serialize_Deserialize_NoColumnNames()
    {
        var original = new ViewInfo(4, "simple_view", "db", "SELECT * FROM t");

        var bytes = original.Serialize();
        var deserialized = ViewInfo.Deserialize(bytes);

        Assert.That(deserialized.ColumnNames, Is.Null);
    }

    [Test]
    public void ViewInfo_Create_ThrowsForEmptyName()
    {
        Assert.Throws<ArgumentException>(() =>
            new ViewInfo(1, "", "db", "SELECT 1"));
    }

    [Test]
    public void ViewInfo_Create_ThrowsForEmptyDefinition()
    {
        Assert.Throws<ArgumentException>(() =>
            new ViewInfo(1, "view1", "db", ""));
    }

    [Test]
    public void DatabaseInfo_AddView_And_GetView()
    {
        var db = new DatabaseInfo(1, "testdb", "/data/testdb");
        var view = new ViewInfo(1, "my_view", "testdb", "SELECT 1");

        db.AddView(view);

        Assert.That(db.HasView("my_view"), Is.True);
        Assert.That(db.GetView("my_view"), Is.Not.Null);
        Assert.That(db.GetView("my_view")!.ViewId, Is.EqualTo(1));
    }

    [Test]
    public void DatabaseInfo_RemoveView()
    {
        var db = new DatabaseInfo(1, "testdb", "/data/testdb");
        var view = new ViewInfo(1, "my_view", "testdb", "SELECT 1");

        db.AddView(view);
        Assert.That(db.HasView("my_view"), Is.True);

        db.RemoveView("my_view");
        Assert.That(db.HasView("my_view"), Is.False);
    }

    [Test]
    public void DatabaseInfo_AddOrReplaceView_ReplacesExisting()
    {
        var db = new DatabaseInfo(1, "testdb", "/data/testdb");

        var view1 = new ViewInfo(1, "my_view", "testdb", "SELECT 1");
        var view2 = new ViewInfo(2, "my_view", "testdb", "SELECT 2");

        db.AddView(view1);
        db.AddOrReplaceView(view2);

        var retrieved = db.GetView("my_view");
        Assert.That(retrieved!.ViewId, Is.EqualTo(2));
        Assert.That(retrieved.Definition, Is.EqualTo("SELECT 2"));
    }

    [Test]
    public void DatabaseInfo_Views_Property_ReturnsAllViews()
    {
        var db = new DatabaseInfo(1, "testdb", "/data/testdb");

        db.AddView(new ViewInfo(1, "view1", "testdb", "SELECT 1"));
        db.AddView(new ViewInfo(2, "view2", "testdb", "SELECT 2"));

        Assert.That(db.Views, Has.Count.EqualTo(2));
    }

    [Test]
    public void DatabaseInfo_Serialize_WithViews()
    {
        var original = new DatabaseInfo(1, "testdb", "/data/testdb");
        original.AddView(new ViewInfo(1, "v1", "testdb", "SELECT a FROM t"));
        original.AddView(new ViewInfo(2, "v2", "testdb", "SELECT b FROM t"));

        var bytes = original.Serialize();
        var deserialized = DatabaseInfo.Deserialize(bytes);

        Assert.That(deserialized.Views, Has.Count.EqualTo(2));
        Assert.That(deserialized.HasView("v1"), Is.True);
        Assert.That(deserialized.HasView("v2"), Is.True);
    }
}
