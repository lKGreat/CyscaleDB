# Redis 命令实际使用示例

本文档提供了 CysRedis 中常用的插入和查询数据命令的实际示例。

## 如何使用

你可以使用 Redis CLI 客户端连接到 CysRedis 服务器，然后执行以下命令。

---

## 1. 字符串（String）操作示例

### 插入数据

```redis
# 基本设置
SET name "张三"
# 返回: OK

SET age "25"
# 返回: OK

# 批量设置
MSET city "北京" country "中国" language "中文"
# 返回: OK

# 仅在键不存在时设置
SETNX name "李四"
# 返回: 0 (因为 name 已存在)

SETNX email "zhangsan@example.com"
# 返回: 1 (设置成功)

# 设置带过期时间（10秒后过期）
SETEX session:12345 10 "user_token_abc"
# 返回: OK

# 设置带过期时间（5000毫秒后过期）
PSETEX temp:key 5000 "temporary_value"
# 返回: OK
```

### 查询数据

```redis
# 获取单个值
GET name
# 返回: "张三"

GET age
# 返回: "25"

# 批量获取
MGET name age city country
# 返回: 
# 1) "张三"
# 2) "25"
# 3) "北京"
# 4) "中国"

# 获取不存在的键
GET nonexistent
# 返回: (nil)

# 获取字符串长度
STRLEN name
# 返回: 6 (中文字符按UTF-8编码计算)

# 获取子串（从索引0到2）
GETRANGE name 0 2
# 返回: "张三"

# 追加字符串
APPEND name "先生"
# 返回: 10 (新长度)
GET name
# 返回: "张三先生"
```

---

## 2. 哈希（Hash）操作示例

### 插入数据

```redis
# 设置单个字段
HSET user:1001 name "张三"
# 返回: 1 (新增字段数)

HSET user:1001 age "25"
# 返回: 1

HSET user:1001 city "北京"
# 返回: 1

# 批量设置多个字段
HMSET user:1002 name "李四" age "30" city "上海" email "lisi@example.com"
# 返回: OK

# 仅在字段不存在时设置
HSETNX user:1001 phone "13800138000"
# 返回: 1 (设置成功)

HSETNX user:1001 name "王五"
# 返回: 0 (字段已存在，未设置)
```

### 查询数据

```redis
# 获取单个字段值
HGET user:1001 name
# 返回: "张三"

HGET user:1001 age
# 返回: "25"

# 批量获取多个字段值
HMGET user:1001 name age city
# 返回:
# 1) "张三"
# 2) "25"
# 3) "北京"

# 获取所有字段和值
HGETALL user:1001
# 返回:
# 1) "name"
# 2) "张三"
# 3) "age"
# 4) "25"
# 5) "city"
# 6) "北京"
# 7) "phone"
# 8) "13800138000"

# 获取所有字段名
HKEYS user:1001
# 返回:
# 1) "name"
# 2) "age"
# 3) "city"
# 4) "phone"

# 获取所有字段值
HVALS user:1001
# 返回:
# 1) "张三"
# 2) "25"
# 3) "北京"
# 4) "13800138000"

# 检查字段是否存在
HEXISTS user:1001 name
# 返回: 1 (存在)

HEXISTS user:1001 address
# 返回: 0 (不存在)

# 获取哈希的字段数量
HLEN user:1001
# 返回: 4

# 字段值递增
HINCRBY user:1001 age 1
# 返回: 26

HGET user:1001 age
# 返回: "26"
```

---

## 3. 列表（List）操作示例

### 插入数据

```redis
# 从左侧（头部）插入元素
LPUSH mylist "item1"
# 返回: 1

LPUSH mylist "item2" "item3"
# 返回: 3

# 从右侧（尾部）插入元素
RPUSH mylist "item4"
# 返回: 4

RPUSH mylist "item5" "item6"
# 返回: 6

# 此时列表顺序: item3, item2, item1, item4, item5, item6
```

### 查询数据

```redis
# 获取列表长度
LLEN mylist
# 返回: 6

# 获取指定范围的元素（0到-1表示全部）
LRANGE mylist 0 -1
# 返回:
# 1) "item3"
# 2) "item2"
# 3) "item1"
# 4) "item4"
# 5) "item5"
# 6) "item6"

# 获取前3个元素
LRANGE mylist 0 2
# 返回:
# 1) "item3"
# 2) "item2"
# 3) "item1"

# 获取最后一个元素
LRANGE mylist -1 -1
# 返回:
# 1) "item6"

# 获取指定索引的元素
LINDEX mylist 0
# 返回: "item3"

LINDEX mylist 2
# 返回: "item1"

LINDEX mylist -1
# 返回: "item6" (负数表示从尾部开始)
```

### 删除数据

```redis
# 从左侧弹出元素
LPOP mylist
# 返回: "item3"

# 从右侧弹出元素
RPOP mylist
# 返回: "item6"

# 弹出多个元素
LPOP mylist 2
# 返回:
# 1) "item2"
# 2) "item1"
```

---

## 4. 集合（Set）操作示例

### 插入数据

```redis
# 添加成员
SADD myset "apple"
# 返回: 1

SADD myset "banana" "orange" "apple"
# 返回: 2 (apple已存在，只新增了2个)

SADD fruits "apple" "banana" "orange" "grape"
# 返回: 4
```

### 查询数据

```redis
# 获取所有成员
SMEMBERS myset
# 返回:
# 1) "apple"
# 2) "banana"
# 3) "orange"

# 检查成员是否存在
SISMEMBER myset "apple"
# 返回: 1 (存在)

SISMEMBER myset "grape"
# 返回: 0 (不存在)

# 获取集合成员数量
SCARD myset
# 返回: 3

# 随机获取一个成员（不删除）
SRANDMEMBER myset
# 返回: "banana" (随机)

# 随机弹出成员（会删除）
SPOP myset
# 返回: "orange"
```

---

## 5. 有序集合（Sorted Set）操作示例

### 插入数据

```redis
# 添加成员及其分数
ZADD leaderboard 100 "player1"
# 返回: 1

ZADD leaderboard 200 "player2" 150 "player3" 300 "player4"
# 返回: 3

# 更新分数（如果成员已存在）
ZADD leaderboard 250 "player1"
# 返回: 0 (成员已存在，更新分数)
```

### 查询数据

```redis
# 按排名范围获取成员（从低到高）
ZRANGE leaderboard 0 -1
# 返回:
# 1) "player3"
# 2) "player1"
# 3) "player2"
# 4) "player4"

# 获取前3名
ZRANGE leaderboard 0 2
# 返回:
# 1) "player3"
# 2) "player1"
# 3) "player2"

# 获取成员及其分数
ZRANGE leaderboard 0 -1 WITHSCORES
# 返回:
# 1) "player3"
# 2) "150"
# 3) "player1"
# 4) "250"
# 5) "player2"
# 6) "200"
# 7) "player4"
# 8) "300"

# 获取成员的分数
ZSCORE leaderboard "player1"
# 返回: "250"

# 获取成员的排名（从0开始，分数从低到高）
ZRANK leaderboard "player1"
# 返回: 1

# 获取成员的排名（从0开始，分数从高到低）
ZREVRANK leaderboard "player1"
# 返回: 2

# 获取有序集合成员数量
ZCARD leaderboard
# 返回: 4

# 增加成员的分数
ZINCRBY leaderboard 50 "player1"
# 返回: "300"

ZSCORE leaderboard "player1"
# 返回: "300"
```

---

## 6. 综合示例：用户管理系统

```redis
# ===== 用户注册 =====
# 创建用户基本信息（使用哈希）
HMSET user:zhangsan name "张三" age "25" email "zhangsan@example.com" city "北京"
# 返回: OK

# 创建用户会话（使用字符串，带过期时间）
SETEX session:zhangsan 3600 "token_abc123xyz"
# 返回: OK

# 记录用户登录历史（使用列表）
LPUSH user:zhangsan:logins "2024-01-15 10:30:00"
LPUSH user:zhangsan:logins "2024-01-14 09:15:00"
# 返回: 2

# 记录用户标签（使用集合）
SADD user:zhangsan:tags "VIP" "北京用户" "活跃用户"
# 返回: 3

# 记录用户积分（使用有序集合）
ZADD user:points 1000 "zhangsan"
ZADD user:points 500 "lisi" 2000 "wangwu"
# 返回: 3

# ===== 查询用户信息 =====
# 获取基本信息
HGETALL user:zhangsan
# 返回: 所有字段和值

# 获取会话
GET session:zhangsan
# 返回: "token_abc123xyz"

# 获取最近3次登录记录
LRANGE user:zhangsan:logins 0 2
# 返回: 最近3次登录时间

# 获取用户标签
SMEMBERS user:zhangsan:tags
# 返回: 所有标签

# 获取积分排行榜前10名
ZREVRANGE user:points 0 9 WITHSCORES
# 返回: 前10名用户及其积分

# ===== 更新用户信息 =====
# 更新年龄
HSET user:zhangsan age "26"
# 返回: 0 (字段已存在，更新)

# 增加积分
ZINCRBY user:points 100 "zhangsan"
# 返回: "1100"

# 添加新标签
SADD user:zhangsan:tags "新用户"
# 返回: 1

# ===== 删除数据 =====
# 删除会话
DEL session:zhangsan
# 返回: 1

# 删除用户标签中的某个标签
SREM user:zhangsan:tags "新用户"
# 返回: 1

# 删除用户积分记录
ZREM user:points "zhangsan"
# 返回: 1
```

---

## 7. 键管理命令示例

```redis
# 检查键是否存在
EXISTS user:zhangsan
# 返回: 1 (存在)

EXISTS user:nonexistent
# 返回: 0 (不存在)

# 获取键的类型
TYPE user:zhangsan
# 返回: "hash"

TYPE mylist
# 返回: "list"

# 设置键的过期时间（秒）
EXPIRE session:zhangsan 3600
# 返回: 1

# 查看键的剩余过期时间（秒）
TTL session:zhangsan
# 返回: 3599

# 查看键的剩余过期时间（毫秒）
PTTL session:zhangsan
# 返回: 3599000

# 移除键的过期时间
PERSIST session:zhangsan
# 返回: 1

# 查找匹配模式的键
KEYS user:*
# 返回: 所有以 "user:" 开头的键

# 删除键
DEL user:zhangsan
# 返回: 1

# 批量删除
DEL key1 key2 key3
# 返回: 删除的键数量
```

---

## 注意事项

1. **数据类型限制**：每个键只能是一种数据类型，不能混用。例如，如果 `mylist` 是列表类型，就不能用 `GET mylist` 来获取。

2. **过期时间**：使用 `SETEX`、`PSETEX` 或 `EXPIRE` 设置的键会在指定时间后自动删除。

3. **列表索引**：列表索引从 0 开始，负数表示从尾部开始（-1 是最后一个元素）。

4. **有序集合排序**：默认按分数从低到高排序，使用 `ZREVRANGE` 可以按分数从高到低排序。

5. **集合去重**：集合中的成员是唯一的，重复添加同一个成员不会增加集合大小。

6. **批量操作**：使用 `MSET`、`MGET`、`HMSET`、`HMGET` 等批量命令可以提高效率。

---

## 连接 CysRedis 服务器

如果你使用 `redis-cli` 连接：

```bash
redis-cli -h localhost -p 6379
```

或者使用其他 Redis 客户端工具连接到 CysRedis 服务器后执行上述命令。
