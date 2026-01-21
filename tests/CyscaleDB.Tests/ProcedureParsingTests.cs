using CyscaleDB.Core.Common;
using CyscaleDB.Core.Parsing;
using CyscaleDB.Core.Parsing.Ast;
using Xunit;

namespace CyscaleDB.Tests;

public class ProcedureParsingTests
{
    [Fact]
    public void ParseSimpleProcedure_ShouldSucceed()
    {
        // Arrange
        var sql = @"
            CREATE PROCEDURE test_proc()
            BEGIN
                SELECT 1;
            END
        ";

        // Act
        var parser = new Parser(sql);
        var statement = parser.Parse();

        // Assert
        Assert.IsType<CreateProcedureStatement>(statement);
        var procStmt = (CreateProcedureStatement)statement;
        Assert.Equal("test_proc", procStmt.ProcedureName);
        Assert.Empty(procStmt.Parameters);
        Assert.Single(procStmt.Body);
        Assert.IsType<SelectStatement>(procStmt.Body[0]);
    }

    [Fact]
    public void ParseProcedureWithParameters_ShouldSucceed()
    {
        // Arrange
        var sql = @"
            CREATE PROCEDURE add_user(IN username VARCHAR(50), IN age INT, OUT user_id INT)
            BEGIN
                INSERT INTO users (name, age) VALUES (username, age);
            END
        ";

        // Act
        var parser = new Parser(sql);
        var statement = parser.Parse();

        // Assert
        Assert.IsType<CreateProcedureStatement>(statement);
        var procStmt = (CreateProcedureStatement)statement;
        Assert.Equal("add_user", procStmt.ProcedureName);
        Assert.Equal(3, procStmt.Parameters.Count);
        
        // Check parameters
        Assert.Equal("username", procStmt.Parameters[0].Name);
        Assert.Equal(ParameterMode.In, procStmt.Parameters[0].Mode);
        Assert.Equal(DataType.VarChar, procStmt.Parameters[0].DataType);
        Assert.Equal(50, procStmt.Parameters[0].Size);
        
        Assert.Equal("age", procStmt.Parameters[1].Name);
        Assert.Equal(ParameterMode.In, procStmt.Parameters[1].Mode);
        Assert.Equal(DataType.Int, procStmt.Parameters[1].DataType);
        
        Assert.Equal("user_id", procStmt.Parameters[2].Name);
        Assert.Equal(ParameterMode.Out, procStmt.Parameters[2].Mode);
        Assert.Equal(DataType.Int, procStmt.Parameters[2].DataType);
    }

    [Fact]
    public void ParseProcedureWithDeclare_ShouldSucceed()
    {
        // Arrange
        var sql = @"
            CREATE PROCEDURE test_variables()
            BEGIN
                DECLARE x INT DEFAULT 10;
                DECLARE y, z VARCHAR(100);
                SET x = 20;
                SELECT x;
            END
        ";

        // Act
        var parser = new Parser(sql);
        var statement = parser.Parse();

        // Assert
        Assert.IsType<CreateProcedureStatement>(statement);
        var procStmt = (CreateProcedureStatement)statement;
        Assert.Equal(4, procStmt.Body.Count);
        
        // Check DECLARE statements
        Assert.IsType<DeclareVariableStatement>(procStmt.Body[0]);
        var declare1 = (DeclareVariableStatement)procStmt.Body[0];
        Assert.Single(declare1.VariableNames);
        Assert.Equal("x", declare1.VariableNames[0]);
        Assert.Equal(DataType.Int, declare1.DataType);
        Assert.NotNull(declare1.DefaultValue);
        
        Assert.IsType<DeclareVariableStatement>(procStmt.Body[1]);
        var declare2 = (DeclareVariableStatement)procStmt.Body[1];
        Assert.Equal(2, declare2.VariableNames.Count);
        Assert.Equal("y", declare2.VariableNames[0]);
        Assert.Equal("z", declare2.VariableNames[1]);
        Assert.Equal(DataType.VarChar, declare2.DataType);
        
        // Check SET statement
        Assert.IsType<SetStatement>(procStmt.Body[2]);
        
        // Check SELECT statement
        Assert.IsType<SelectStatement>(procStmt.Body[3]);
    }

    [Fact]
    public void ParseCallStatement_ShouldSucceed()
    {
        // Arrange
        var sql = "CALL add_user('John', 25, 100)";

        // Act
        var parser = new Parser(sql);
        var statement = parser.Parse();

        // Assert
        Assert.IsType<CallStatement>(statement);
        var callStmt = (CallStatement)statement;
        Assert.Equal("add_user", callStmt.ProcedureName);
        Assert.Equal(3, callStmt.Arguments.Count);
    }

    [Fact]
    public void ParseProcedureWithCharacteristics_ShouldSucceed()
    {
        // Arrange
        var sql = @"
            CREATE PROCEDURE test_proc()
            COMMENT 'Test procedure'
            SQL SECURITY DEFINER
            BEGIN
                SELECT 1;
            END
        ";

        // Act
        var parser = new Parser(sql);
        var statement = parser.Parse();

        // Assert
        Assert.IsType<CreateProcedureStatement>(statement);
        var procStmt = (CreateProcedureStatement)statement;
        Assert.Equal("test_proc", procStmt.ProcedureName);
        Assert.Equal("Test procedure", procStmt.Comment);
        Assert.Equal("DEFINER", procStmt.SqlSecurity);
    }

    [Fact]
    public void ParseProcedureWithOrReplace_ShouldSucceed()
    {
        // Arrange
        var sql = @"
            CREATE OR REPLACE PROCEDURE test_proc()
            BEGIN
                SELECT 1;
            END
        ";

        // Act
        var parser = new Parser(sql);
        var statement = parser.Parse();

        // Assert
        Assert.IsType<CreateProcedureStatement>(statement);
        var procStmt = (CreateProcedureStatement)statement;
        Assert.True(procStmt.OrReplace);
        Assert.Equal("test_proc", procStmt.ProcedureName);
    }

    [Fact]
    public void ParseProcedureWithInOutParameter_ShouldSucceed()
    {
        // Arrange
        var sql = @"
            CREATE PROCEDURE update_counter(INOUT counter INT)
            BEGIN
                SET counter = counter + 1;
            END
        ";

        // Act
        var parser = new Parser(sql);
        var statement = parser.Parse();

        // Assert
        Assert.IsType<CreateProcedureStatement>(statement);
        var procStmt = (CreateProcedureStatement)statement;
        Assert.Single(procStmt.Parameters);
        Assert.Equal("counter", procStmt.Parameters[0].Name);
        Assert.Equal(ParameterMode.InOut, procStmt.Parameters[0].Mode);
        Assert.Equal(DataType.Int, procStmt.Parameters[0].DataType);
    }

    [Fact]
    public void ParseProcedureWithMultipleStatements_ShouldSucceed()
    {
        // Arrange
        var sql = @"
            CREATE PROCEDURE complex_proc(IN x INT)
            BEGIN
                DECLARE result INT;
                SET result = x * 2;
                INSERT INTO results (value) VALUES (result);
                SELECT result;
            END
        ";

        // Act
        var parser = new Parser(sql);
        var statement = parser.Parse();

        // Assert
        Assert.IsType<CreateProcedureStatement>(statement);
        var procStmt = (CreateProcedureStatement)statement;
        Assert.Equal("complex_proc", procStmt.ProcedureName);
        Assert.Equal(4, procStmt.Body.Count);
        Assert.IsType<DeclareVariableStatement>(procStmt.Body[0]);
        Assert.IsType<SetStatement>(procStmt.Body[1]);
        Assert.IsType<InsertStatement>(procStmt.Body[2]);
        Assert.IsType<SelectStatement>(procStmt.Body[3]);
    }
}
