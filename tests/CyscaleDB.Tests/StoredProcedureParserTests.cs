using Xunit;
using CyscaleDB.Core.Parsing;
using CyscaleDB.Core.Parsing.Ast;
using CyscaleDB.Core.Common;

namespace CyscaleDB.Tests;

/// <summary>
/// Tests for stored procedure parsing (Phase 3.1 - Tasks 3.1.1 to 3.1.6).
/// </summary>
public class StoredProcedureParserTests
{
    [Fact]
    public void ParseCreateProcedure_SimpleWithNoParameters_Success()
    {
        // Arrange
        var sql = "CREATE PROCEDURE test_proc() BEGIN SELECT 1; END;";
        var parser = new Parser(sql);

        // Act
        var statement = parser.Parse();

        // Assert
        Assert.IsType<CreateProcedureStatement>(statement);
        var proc = (CreateProcedureStatement)statement;
        Assert.Equal("test_proc", proc.ProcedureName);
        Assert.Empty(proc.Parameters);
        Assert.Single(proc.Body);
    }

    [Fact]
    public void ParseCreateProcedure_WithInParameter_Success()
    {
        // Arrange
        var sql = "CREATE PROCEDURE test_proc(IN p_id INT) BEGIN SELECT 1; END;";
        var parser = new Parser(sql);

        // Act
        var statement = parser.Parse();

        // Assert
        Assert.IsType<CreateProcedureStatement>(statement);
        var proc = (CreateProcedureStatement)statement;
        Assert.Equal("test_proc", proc.ProcedureName);
        Assert.Single(proc.Parameters);
        Assert.Equal("p_id", proc.Parameters[0].Name);
        Assert.Equal(ParameterMode.In, proc.Parameters[0].Mode);
        Assert.Equal(DataType.Int, proc.Parameters[0].DataType);
    }

    [Fact]
    public void ParseCreateProcedure_WithOutParameter_Success()
    {
        // Arrange
        var sql = "CREATE PROCEDURE test_proc(OUT p_name VARCHAR(100)) BEGIN SELECT 1; END;";
        var parser = new Parser(sql);

        // Act
        var statement = parser.Parse();

        // Assert
        Assert.IsType<CreateProcedureStatement>(statement);
        var proc = (CreateProcedureStatement)statement;
        Assert.Single(proc.Parameters);
        Assert.Equal("p_name", proc.Parameters[0].Name);
        Assert.Equal(ParameterMode.Out, proc.Parameters[0].Mode);
        Assert.Equal(DataType.VarChar, proc.Parameters[0].DataType);
        Assert.Equal(100, proc.Parameters[0].Size);
    }

    [Fact]
    public void ParseCreateProcedure_WithInOutParameter_Success()
    {
        // Arrange
        var sql = "CREATE PROCEDURE test_proc(INOUT p_value INT) BEGIN SELECT 1; END;";
        var parser = new Parser(sql);

        // Act
        var statement = parser.Parse();

        // Assert
        Assert.IsType<CreateProcedureStatement>(statement);
        var proc = (CreateProcedureStatement)statement;
        Assert.Single(proc.Parameters);
        Assert.Equal("p_value", proc.Parameters[0].Name);
        Assert.Equal(ParameterMode.InOut, proc.Parameters[0].Mode);
    }

    [Fact]
    public void ParseCreateProcedure_WithMultipleParameters_Success()
    {
        // Arrange
        var sql = "CREATE PROCEDURE test_proc(IN p_id INT, OUT p_name VARCHAR(100), INOUT p_count BIGINT) BEGIN SELECT 1; END;";
        var parser = new Parser(sql);

        // Act
        var statement = parser.Parse();

        // Assert
        Assert.IsType<CreateProcedureStatement>(statement);
        var proc = (CreateProcedureStatement)statement;
        Assert.Equal(3, proc.Parameters.Count);
        Assert.Equal("p_id", proc.Parameters[0].Name);
        Assert.Equal("p_name", proc.Parameters[1].Name);
        Assert.Equal("p_count", proc.Parameters[2].Name);
    }

    [Fact]
    public void ParseCreateProcedure_WithDeclareStatement_Success()
    {
        // Arrange
        var sql = "CREATE PROCEDURE test_proc() BEGIN DECLARE v_temp INT; SELECT 1; END;";
        var parser = new Parser(sql);

        // Act
        var statement = parser.Parse();

        // Assert
        Assert.IsType<CreateProcedureStatement>(statement);
        var proc = (CreateProcedureStatement)statement;
        Assert.Equal(2, proc.Body.Count);
        Assert.IsType<DeclareVariableStatement>(proc.Body[0]);
        var declare = (DeclareVariableStatement)proc.Body[0];
        Assert.Single(declare.VariableNames);
        Assert.Equal("v_temp", declare.VariableNames[0]);
        Assert.Equal(DataType.Int, declare.DataType);
    }

    [Fact]
    public void ParseCreateProcedure_WithDeclareAndDefault_Success()
    {
        // Arrange
        var sql = "CREATE PROCEDURE test_proc() BEGIN DECLARE v_count INT DEFAULT 0; END;";
        var parser = new Parser(sql);

        // Act
        var statement = parser.Parse();

        // Assert
        Assert.IsType<CreateProcedureStatement>(statement);
        var proc = (CreateProcedureStatement)statement;
        Assert.Single(proc.Body);
        Assert.IsType<DeclareVariableStatement>(proc.Body[0]);
        var declare = (DeclareVariableStatement)proc.Body[0];
        Assert.NotNull(declare.DefaultValue);
    }

    [Fact]
    public void ParseCreateProcedure_WithMultipleVariablesDeclare_Success()
    {
        // Arrange
        var sql = "CREATE PROCEDURE test_proc() BEGIN DECLARE v1, v2, v3 INT; END;";
        var parser = new Parser(sql);

        // Act
        var statement = parser.Parse();

        // Assert
        Assert.IsType<CreateProcedureStatement>(statement);
        var proc = (CreateProcedureStatement)statement;
        Assert.Single(proc.Body);
        Assert.IsType<DeclareVariableStatement>(proc.Body[0]);
        var declare = (DeclareVariableStatement)proc.Body[0];
        Assert.Equal(3, declare.VariableNames.Count);
        Assert.Equal("v1", declare.VariableNames[0]);
        Assert.Equal("v2", declare.VariableNames[1]);
        Assert.Equal("v3", declare.VariableNames[2]);
    }

    [Fact]
    public void ParseCreateProcedure_WithSetStatement_Success()
    {
        // Arrange
        var sql = "CREATE PROCEDURE test_proc() BEGIN DECLARE v_count INT; SET v_count = 10; END;";
        var parser = new Parser(sql);

        // Act
        var statement = parser.Parse();

        // Assert
        Assert.IsType<CreateProcedureStatement>(statement);
        var proc = (CreateProcedureStatement)statement;
        Assert.Equal(2, proc.Body.Count);
        Assert.IsType<DeclareVariableStatement>(proc.Body[0]);
        Assert.IsType<SetStatement>(proc.Body[1]);
        var setStmt = (SetStatement)proc.Body[1];
        Assert.Single(setStmt.Variables);
        Assert.Equal("v_count", setStmt.Variables[0].Name);
    }

    [Fact]
    public void ParseCallStatement_WithNoArguments_Success()
    {
        // Arrange
        var sql = "CALL test_proc();";
        var parser = new Parser(sql);

        // Act
        var statement = parser.Parse();

        // Assert
        Assert.IsType<CallStatement>(statement);
        var call = (CallStatement)statement;
        Assert.Equal("test_proc", call.ProcedureName);
        Assert.Empty(call.Arguments);
    }

    [Fact]
    public void ParseCallStatement_WithArguments_Success()
    {
        // Arrange
        var sql = "CALL test_proc(1, 'test', @var);";
        var parser = new Parser(sql);

        // Act
        var statement = parser.Parse();

        // Assert
        Assert.IsType<CallStatement>(statement);
        var call = (CallStatement)statement;
        Assert.Equal("test_proc", call.ProcedureName);
        Assert.Equal(3, call.Arguments.Count);
    }

    [Fact]
    public void ParseCreateProcedure_ComplexExample_Success()
    {
        // Arrange
        var sql = @"CREATE PROCEDURE GetUserInfo(IN user_id INT, OUT user_name VARCHAR(100))
BEGIN
    DECLARE v_count INT DEFAULT 0;
    DECLARE v_temp VARCHAR(50);
    
    SET v_count = 100;
    SET v_temp = 'test';
    
    SELECT name INTO user_name FROM users WHERE id = user_id;
    SELECT COUNT(*) INTO v_count FROM orders WHERE user_id = user_id;
END;";
        var parser = new Parser(sql);

        // Act
        var statement = parser.Parse();

        // Assert
        Assert.IsType<CreateProcedureStatement>(statement);
        var proc = (CreateProcedureStatement)statement;
        Assert.Equal("GetUserInfo", proc.ProcedureName);
        Assert.Equal(2, proc.Parameters.Count);
        Assert.True(proc.Body.Count >= 4); // At least 2 DECLARE + 2 SET statements
    }

    [Fact]
    public void ParseIfStatement_Simple_Success()
    {
        // Arrange
        var sql = @"CREATE PROCEDURE test_if(IN p_value INT)
BEGIN
    IF p_value > 0 THEN
        SELECT 'positive';
    END IF;
END;";
        var parser = new Parser(sql);

        // Act
        var statement = parser.Parse();

        // Assert
        Assert.IsType<CreateProcedureStatement>(statement);
        var proc = (CreateProcedureStatement)statement;
        Assert.Single(proc.Body);
        Assert.IsType<IfStatement>(proc.Body[0]);
        var ifStmt = (IfStatement)proc.Body[0];
        Assert.NotNull(ifStmt.Condition);
        Assert.Single(ifStmt.ThenStatements);
    }

    [Fact]
    public void ParseIfStatement_WithElseIf_Success()
    {
        // Arrange
        var sql = @"CREATE PROCEDURE test_if(IN p_value INT)
BEGIN
    IF p_value > 0 THEN
        SELECT 'positive';
    ELSEIF p_value < 0 THEN
        SELECT 'negative';
    ELSE
        SELECT 'zero';
    END IF;
END;";
        var parser = new Parser(sql);

        // Act
        var statement = parser.Parse();

        // Assert
        Assert.IsType<CreateProcedureStatement>(statement);
        var proc = (CreateProcedureStatement)statement;
        Assert.Single(proc.Body);
        Assert.IsType<IfStatement>(proc.Body[0]);
        var ifStmt = (IfStatement)proc.Body[0];
        Assert.NotNull(ifStmt.Condition);
        Assert.Single(ifStmt.ThenStatements);
        Assert.Single(ifStmt.ElseIfClauses);
        Assert.NotNull(ifStmt.ElseStatements);
        Assert.Single(ifStmt.ElseStatements);
    }

    [Fact]
    public void ParseWhileStatement_Success()
    {
        // Arrange
        var sql = @"CREATE PROCEDURE test_while()
BEGIN
    DECLARE v_count INT DEFAULT 0;
    WHILE v_count < 10 DO
        SET v_count = v_count + 1;
    END WHILE;
END;";
        var parser = new Parser(sql);

        // Act
        var statement = parser.Parse();

        // Assert
        Assert.IsType<CreateProcedureStatement>(statement);
        var proc = (CreateProcedureStatement)statement;
        Assert.Equal(2, proc.Body.Count);
        Assert.IsType<WhileStatement>(proc.Body[1]);
        var whileStmt = (WhileStatement)proc.Body[1];
        Assert.NotNull(whileStmt.Condition);
        Assert.Single(whileStmt.Body);
    }

    [Fact]
    public void ParseDropProcedure_Simple_Success()
    {
        // Arrange
        var sql = "DROP PROCEDURE test_proc;";
        var parser = new Parser(sql);

        // Act
        var statement = parser.Parse();

        // Assert
        Assert.IsType<DropProcedureStatement>(statement);
        var drop = (DropProcedureStatement)statement;
        Assert.Equal("test_proc", drop.ProcedureName);
        Assert.False(drop.IfExists);
    }

    [Fact]
    public void ParseDropProcedure_IfExists_Success()
    {
        // Arrange
        var sql = "DROP PROCEDURE IF EXISTS test_proc;";
        var parser = new Parser(sql);

        // Act
        var statement = parser.Parse();

        // Assert
        Assert.IsType<DropProcedureStatement>(statement);
        var drop = (DropProcedureStatement)statement;
        Assert.Equal("test_proc", drop.ProcedureName);
        Assert.True(drop.IfExists);
    }

    [Fact]
    public void ParseCreateFunction_Simple_Success()
    {
        // Arrange
        var sql = @"CREATE FUNCTION test_func(p_value INT) RETURNS INT
BEGIN
    RETURN p_value * 2;
END;";
        var parser = new Parser(sql);

        // Act
        var statement = parser.Parse();

        // Assert
        Assert.IsType<CreateFunctionStatement>(statement);
        var func = (CreateFunctionStatement)statement;
        Assert.Equal("test_func", func.FunctionName);
        Assert.Single(func.Parameters);
        Assert.Equal("p_value", func.Parameters[0].Name);
        Assert.Equal(DataType.Int, func.ReturnType);
        Assert.Single(func.Body);
    }

    [Fact]
    public void ParseCreateFunction_Deterministic_Success()
    {
        // Arrange
        var sql = @"CREATE FUNCTION add_numbers(a INT, b INT) RETURNS INT DETERMINISTIC
BEGIN
    RETURN a + b;
END;";
        var parser = new Parser(sql);

        // Act
        var statement = parser.Parse();

        // Assert
        Assert.IsType<CreateFunctionStatement>(statement);
        var func = (CreateFunctionStatement)statement;
        Assert.Equal("add_numbers", func.FunctionName);
        Assert.Equal(2, func.Parameters.Count);
        Assert.Equal(DataType.Int, func.ReturnType);
        Assert.True(func.IsDeterministic);
    }

    [Fact]
    public void ParseCreateFunction_WithVarcharReturn_Success()
    {
        // Arrange
        var sql = @"CREATE FUNCTION get_name(id INT) RETURNS VARCHAR(100)
BEGIN
    DECLARE result VARCHAR(100);
    SELECT name INTO result FROM users WHERE user_id = id;
    RETURN result;
END;";
        var parser = new Parser(sql);

        // Act
        var statement = parser.Parse();

        // Assert
        Assert.IsType<CreateFunctionStatement>(statement);
        var func = (CreateFunctionStatement)statement;
        Assert.Equal("get_name", func.FunctionName);
        Assert.Single(func.Parameters);
        Assert.Equal(DataType.VarChar, func.ReturnType);
        Assert.Equal(100, func.ReturnSize);
        Assert.Equal(3, func.Body.Count);
    }
}
