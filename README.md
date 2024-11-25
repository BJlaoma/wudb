# 整体设计

## 系统模块划分

### Storage Management Module

    File Management：负责物理存储的分配和回收，以及文件的读写操作。
    Buffer Management：管理内存中的数据缓存，提高数据访问速度。
    Transaction Management：处理事务的开始、提交和回滚，确保数据的一致性和完整性。

### Query Processing Module：（Compiler）

    Query Parser：将 SQL 查询语句解析成内部表示形式。
    Query Optimizer：选择最优的查询执行计划，以提高查询效率。
    Query Execution Engine：根据优化后的执行计划，实际执行查询操作。

### Transaction Management Module

    Concurrency Control：管理多个事务的并发执行，防止数据冲突。
    Recovery Management：处理系统故障后的数据恢复，确保数据的一致性。

### Security and Authorization Management Module

    User Authentication：验证用户身份，确保只有授权用户可以访问数据库。
    Access Control：管理用户的访问权限，包括读、写、修改等操作。

### Catalog Management Module

    Metadata Management：存储和管理数据库的元数据信息，如表结构、索引等。
    Schema Management：管理数据库的逻辑结构，包括表、视图、索引等。

### Backup and Recovery Module

    Backup Management：定期备份数据库，以防止数据丢失。
    Recovery Management：在系统故障后，从备份中恢复数据。

### Utility Module

    Data Import/Export：提供工具用于数据的导入和导出。
    Performance Monitoring：监控数据库的性能指标，帮助优化系统性能。
    Log Management：记录数据库的操作日志，用于审计和故障分析。

### Interface Module

    Application Programming Interface (API)：提供编程接口，允许应用程序与数据库进行交互。
    User Interface：提供图形化用户界面，方便用户管理和操作数据库。

## 实现计划

### 1. 实现Storage Management Module

#### 文件管理

    文件管理：设计文件存储格式和结构，决定如何将数据存储到文件中。
    储存方式：分页储存
    文件组织方式：每个文件由多个页面组成，每个页面包含固定大小的数据块。
    类：FileManager，PageManager

##### 文件头

    文件头：记录文件的元数据信息，如文件大小、页面大小、页面数量等。
    使用结构体表示：FileHeader
    写入方式：二进制写入，小端序
    初始化：在文件创建时初始化，使用FileHeader.Init()方法
    验证魔数：在读取文件头时验证魔数是否正确

##### 页面

    页面：文件中的数据块，每个页面包含固定大小的数据。

#### 缓冲区管理

    缓冲区管理：实现一个简单的缓冲区管理机制，提高数据访问速度。

#### 事务管理

    事务管理：处理事务的开始、提交和回滚，确保数据的一致性和完整性。

#### 索引管理

    索引管理：设计和实现索引结构，如 B-Tree 或哈希索引，以加速查询。

### 2. 实现Query Processing Module

    查询解析器：实现一个简单的查询解析器，将 SQL 语句解析成内部表示形式。
    查询优化器：设计一个简单的查询优化器，选择最优的查询执行计划。
    查询执行引擎：实现查询执行引擎，根据优化后的执行计划执行查询操作。

### 3. 实现Transaction Management Module

### 4. 实现Security and Authorization Management Module

### 5. 实现Catalog Management Module

### 6. 实现Backup and Recovery Module

### 7. 实现Utility Module

### 8. 实现Interface Module
