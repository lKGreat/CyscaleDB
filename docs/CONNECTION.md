# CyscaleDB 连接指南

本文档介绍如何连接到 CyscaleDB 服务器。

## 服务器启动

### 启动服务器

```bash
# 使用默认端口 3306 和数据目录
dotnet run --project src/CyscaleDB.Server

# 指定自定义数据目录
dotnet run --project src/CyscaleDB.Server ./custom_data_dir

# 或直接运行编译后的程序
dotnet build
./src/CyscaleDB.Server/bin/Debug/net8.0/CyscaleDB.Server.exe [data_directory]
```

服务器启动后，会在默认端口 **3306** 上监听连接。

## 连接参数

### 默认连接参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| 主机 | `127.0.0.1` 或 `localhost` | 服务器地址 |
| 端口 | `3306` | MySQL 协议端口 |
| 用户名 | `root` | 默认用户 |
| 密码 | 空（无密码） | 默认无密码 |
| SSL | 禁用 | 当前版本不支持 SSL |

### 连接字符串格式

#### 标准 MySQL 连接字符串

```
Server=127.0.0.1;Port=3306;Database=testdb;User Id=root;Password=;SslMode=None;AllowPublicKeyRetrieval=true;ConnectionTimeout=5;
```

#### 参数说明

- `Server` - 服务器地址（IP 或主机名）
- `Port` - 端口号（默认 3306）
- `Database` - 数据库名称（可选，可在连接后使用 `USE database`）
- `User Id` - 用户名（默认 `root`）
- `Password` - 密码（默认空）
- `SslMode` - SSL 模式（设置为 `None`，当前版本不支持 SSL）
- `AllowPublicKeyRetrieval` - 允许公钥检索（设置为 `true`）
- `ConnectionTimeout` - 连接超时时间（秒）

## 客户端连接示例

### 1. 使用 MySqlConnector (.NET)

```csharp
using MySqlConnector;

var connectionString = "Server=127.0.0.1;Port=3306;Database=testdb;User Id=root;Password=;SslMode=None;AllowPublicKeyRetrieval=true;ConnectionTimeout=5;";

await using var connection = new MySqlConnection(connectionString);
await connection.OpenAsync();

// 执行查询
await using var cmd = new MySqlCommand("SELECT 1", connection);
var result = await cmd.ExecuteScalarAsync();
Console.WriteLine($"Result: {result}");
```

### 2. 使用 mysql 命令行客户端

```bash
# 连接到服务器（无密码）
mysql -h 127.0.0.1 -P 3306 -u root

# 连接到指定数据库
mysql -h 127.0.0.1 -P 3306 -u root testdb

# 使用密码（如果设置了密码）
mysql -h 127.0.0.1 -P 3306 -u root -p
```

### 3. 使用 MySQL Workbench

1. 打开 MySQL Workbench
2. 点击 "New Connection"
3. 配置连接参数：
   - **Connection Name**: CyscaleDB
   - **Hostname**: `127.0.0.1`
   - **Port**: `3306`
   - **Username**: `root`
   - **Password**: （留空）
   - **Default Schema**: （可选，如 `testdb`）
4. 点击 "Test Connection" 测试连接
5. 点击 "OK" 保存并连接

### 4. 使用 Navicat

1. 打开 Navicat
2. 点击 "连接" -> "MySQL"
3. 配置连接参数：
   - **连接名**: CyscaleDB
   - **主机**: `127.0.0.1`
   - **端口**: `3306`
   - **用户名**: `root`
   - **密码**: （留空）
   - **数据库**: （可选）
4. 点击 "测试连接"
5. 点击 "确定" 保存并连接

### 5. 使用 DBeaver

1. 打开 DBeaver
2. 点击 "新建连接" -> 选择 "MySQL"
3. 配置连接参数：
   - **主机**: `127.0.0.1`
   - **端口**: `3306`
   - **数据库**: （可选）
   - **用户名**: `root`
   - **密码**: （留空）
4. 在 "驱动属性" 中设置：
   - `useSSL`: `false`
   - `allowPublicKeyRetrieval`: `true`
5. 点击 "测试连接"
6. 点击 "完成" 保存并连接

### 6. 使用 Python (mysql-connector-python)

```python
import mysql.connector

config = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': '',
    'database': 'testdb',
    'ssl_disabled': True,
    'allow_public_key_retrieval': True
}

connection = mysql.connector.connect(**config)
cursor = connection.cursor()

cursor.execute("SELECT 1")
result = cursor.fetchone()
print(f"Result: {result}")

cursor.close()
connection.close()
```

### 7. 使用 Node.js (mysql2)

```javascript
const mysql = require('mysql2/promise');

const connection = await mysql.createConnection({
  host: '127.0.0.1',
  port: 3306,
  user: 'root',
  password: '',
  database: 'testdb',
  ssl: false
});

const [rows] = await connection.execute('SELECT 1');
console.log('Result:', rows);

await connection.end();
```

### 8. 使用 JDBC (Java)

```java
import java.sql.*;

String url = "jdbc:mysql://127.0.0.1:3306/testdb?useSSL=false&allowPublicKeyRetrieval=true";
String user = "root";
String password = "";

Connection conn = DriverManager.getConnection(url, user, password);
Statement stmt = conn.createStatement();
ResultSet rs = stmt.executeQuery("SELECT 1");

if (rs.next()) {
    System.out.println("Result: " + rs.getInt(1));
}

rs.close();
stmt.close();
conn.close();
```

## 连接测试

### 快速测试连接

使用 `mysql` 命令行客户端测试连接：

```bash
mysql -h 127.0.0.1 -P 3306 -u root -e "SELECT VERSION();"
```

如果连接成功，应该看到类似输出：
```
+------------------+
| VERSION()        |
+------------------+
| 8.0.0-CyscaleDB  |
+------------------+
```

### 使用 C# 测试连接

```csharp
using MySqlConnector;

var connectionString = "Server=127.0.0.1;Port=3306;User Id=root;Password=;SslMode=None;AllowPublicKeyRetrieval=true;";

try
{
    await using var connection = new MySqlConnection(connectionString);
    await connection.OpenAsync();
    Console.WriteLine("连接成功！");
    
    await using var cmd = new MySqlCommand("SELECT VERSION()", connection);
    var version = await cmd.ExecuteScalarAsync();
    Console.WriteLine($"服务器版本: {version}");
}
catch (Exception ex)
{
    Console.WriteLine($"连接失败: {ex.Message}");
}
```

## 常见问题

### 1. 连接被拒绝

**问题**: `Can't connect to MySQL server on '127.0.0.1'`

**解决方案**:
- 确认服务器已启动
- 检查端口是否正确（默认 3306）
- 检查防火墙设置
- 确认服务器监听地址（默认监听所有接口 `0.0.0.0`）

### 2. 认证失败

**问题**: `Access denied for user 'root'@'localhost'`

**解决方案**:
- 确认用户名正确（默认 `root`）
- 确认密码为空（如果设置了密码，使用正确密码）
- 检查用户权限配置

### 3. 数据库不存在

**问题**: `Unknown database 'testdb'`

**解决方案**:
- 先创建数据库：`CREATE DATABASE testdb;`
- 或在连接字符串中不指定数据库，连接后使用 `USE database_name;`

### 4. SSL 相关错误

**问题**: SSL 连接错误

**解决方案**:
- 在连接字符串中设置 `SslMode=None`
- 当前版本不支持 SSL，必须禁用 SSL

## 服务器日志

服务器启动后会输出日志信息，包括：

```
CyscaleDB Server v8.0.0-CyscaleDB
Starting server on port 3306...
Using data directory: D:\Code\CyscaleDB\data
Initializing storage engine...
Storage engine initialized
Initializing transaction manager...
Transaction manager initialized
Starting MySQL protocol server...
MySQL protocol server started
Server is ready to accept connections.
Press Ctrl+C to shutdown.
```

当客户端连接时，会看到类似日志：

```
Client connected from 127.0.0.1:xxxxx
Client authenticated: username=root, database=testdb
```

## 相关文档

- [项目状态](./PROJECT_STATUS.md) - 了解项目当前状态
- [功能能力](./CAPABILITIES.md) - 查看支持的 SQL 功能
- [架构文档](./ARCHITECTURE.md) - 了解系统架构
