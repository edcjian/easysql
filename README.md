## 项目介绍
本项目是一个使用Kotlin语言编写，基于阿里druid项目的sql解析模块构建的sql构建工具，可以使用链式调用构建较为复杂的跨数据库sql语句（目前支持mysql、pgsql、oracle、hive）。
<br>本项目十分轻量级，除了kotlin基础库和druid支持外，没有任何库引用。
<br>因为数据库增删改查语句中，SELECT语句最为复杂，所以此项目重点为SELECT语句的拼装。
<br>**在此感谢温绍锦先生和他的团队开发出了如此优秀的跨数据库sql parser。**
<br><br>
新版本已经完整支持对象映射特性了哦(#### 对象映射：)
## 快速开始
#### 来看一个构建简单sql的例子：

    val select = Select().from("table").sql()

生成的sql语句：

    SELECT *
    FROM table

#### 支持筛选列：

    val select = Select().from("table").select("a" alias "c1", "b" alias "c2").sql()

生成的sql语句：

    SELECT a AS c1, b AS c2
    FROM table

注：<br>
1.select可以接收若干个字符串，也可以接收若干个复杂查询（后文会介绍）。<br>
2.此处的alias是对String类和Query类实现的扩展函数，使用了Kotlin的扩展函数和中缀函数语法，用来起别名（数据库的AS子句）。

#### 当然也支持SELECT语句的各种功能：

    val select = Select()
                .from("table")
                .select("a" alias "c1", "b" alias "c2")
                .where(column("id") eq "1")
                .orderByAsc("a")
                .limit(10, 100)
                .sql()

生成的sql语句：

    SELECT a AS c1, b AS c2
    FROM table
    WHERE id = '1'
    ORDER BY a ASC
    LIMIT 100, 10

注：<br>
1.where中使用链式调用的语法，后文会介绍。<br>
2.提供orderByAsc和orderByDesc两个函数排序。

#### 跨数据库支持：
创建Select对象的时候可以指定数据库类型（默认为mysql）：

    val select = Select(DB.PGSQL)
                .from("table")
                .limit(10, 100)
                .sql()

生成的sql语句：

    SELECT *
    FROM table
    LIMIT 10 OFFSET 100

#### 聚合函数：

    val select = Select()
                .from("table")
                .select(count())
                .groupBy("column")
                .sql()

生成的sql语句：

    SELECT COUNT(*)
    FROM table
    GROUP BY column

支持的聚合函数有count、countDistinct、sum、avg、max、min。

#### 二元操作符：
可以使用二元操作符构建复杂的sql语句：

    val select = Select()
                .from("table")
                .select((column("a") + column("b")) / 2 alias "col")
                .where((column("a") eq 1) and (column("b") eq 2))
                .sql()

生成的sql语句：

    SELECT (a + b) / 2 AS col
    FROM table
    WHERE a = 1
	    AND b = 2

注：<br>
1.计算用的二元操作符（+、-、*、/、%）使用Kotlin的运算符重载，因为Java中不支持此功能，所以Java调用的时候需要使用.plus()等方式。<br>

2.列名使用column()函数包装；常量使用const()函数（Java调用使用value()函数），当然大多数情况下都可以省略。列、常量、聚合函数等类型因为继承了公共父类Query，所以均可使用二元运算符自由组合。<br>

3.支持以下逻辑运算符：eq(=)、ne(!=)、gt(>)、ge(>=)、lt(<)、le(<=)、and(AND)、or(OR)、xor(XOR)。<br>

4.因为Kotlin不支持自定义运算符的优先级，所以在使用and、or等符号，或者运算符的右侧为多个常量时，每一组运算都需要放在括号中（如果只是and条件，可以用多个where函数调用的方式）。<br>

5.Java调用中缀函数时，比如 a eq b，需要写成eq(a, b)。<br>

#### 其他操作符：
支持inList(IN)、notInList(NOT IN)、like(LIKE)、notLike(NOT LIKE)、isNull(IS NULL)、isNotNull(IS NOT NULL)。

    val select = Select()
                .from("table")
                .where(column("a") inList listOf(1, 2))
                .where(column("b").isNotNull())
                .where(column("c") like "%value%")
                .sql()

生成的sql语句：

    SELECT *
    FROM table
    WHERE a IN (1, 2)
	    AND b IS NOT NULL
	    AND c LIKE '%value%'

#### JOIN子句：
支持的join类型有：join、innerJoin、leftJoin、rightJoin、crossJoin、fullJoin等。

    val select = Select()
                .from("t1")
                .leftJoin("t2", on = column("t1.id") eq column("t2.id"))
                .sql()

生成的sql语句：

    SELECT *
    FROM t1
	    LEFT JOIN t2 ON t1.id = t2.id

#### CASE WHEN子句：
使用case()函数和中缀函数then与elseIs构建一个CASE WHEN子句：

    val select = Select()
                .from("user")
                .select(case(column("gender") eq 1 then "男", column("gender") eq 2 then "女") elseIs "其他" alias "gender")
                .sql()

生成的sql语句：

    SELECT CASE 
		WHEN gender = 1 THEN '男'
		WHEN gender = 2 THEN '女'
		ELSE '其他'
	END AS gender
    FROM user

当然case()函数也可以传入count()和sum()中：

    val case = case(column("gender") eq 1 then column("gender")) elseIs null
    val select = Select()
                .from("user")
                .select(count(case) alias "male_count")
                .sql()

生成的sql语句：

    SELECT COUNT(CASE 
		WHEN gender = 1 THEN gender
		ELSE NULL
	    END) AS male_count
    FROM user

#### UNION和UNION ALL：

    val select = (Select().from("t1").select("a") union
                Select().from("t2").select("a") unionAll
                Select().from("t3").select("a")).sql()

生成的sql语句：

    SELECT a
    FROM t1
    UNION
    SELECT a
    FROM t2
    UNION ALL
    SELECT a
    FROM t3

#### 子查询：
支持from、join、各种操作符的**右侧**使用子查询：

**from中的子查询：**

    val select = Select().from(Select().from("table")).sql()

生成的sql语句：

    SELECT *
    FROM (
	    SELECT 
	    FROM table
    )

**join中的子查询：**

    val select = Select()
                .from("t1")
                .leftJoin(Select().from("t2").limit(1), alias = "t2", on = column("t1.id") eq column("t2.id"))
                .sql()

生成的sql语句：

    SELECT *
    FROM t1
	    LEFT JOIN (
		    SELECT 
		    FROM t2
		    LIMIT 0, 1
	    ) t2
	    ON t1.id = t2.id

**操作符右侧的子查询：**

    val select = Select()
                .from("t1")
                .select(column("id") inList Select().from("t2").select("id"))
                .sql()

生成的sql语句：

    SELECT id IN (
		    SELECT id
		    FROM t2
	    )
    FROM t1

#### 常用数据库函数：

**concat和concatWs：**

concat是一个可变长参数的函数，接收的参数为各种字段、常量等Query的子类型。<br>
concatWs的第一个参数为分隔符的字符串，其他同concat。

    val select = Select()
                .from("table")
                .select(concat(column("a"), const(","), column("b")))
                .select(concatWs(",", column("a"), column("b")))
                .sql()

生成的sql语句：

    SELECT CONCAT(a, ',', b)
	    , CONCAT_WS(',', a, b)
    FROM table

注：以上函数暂不支持oracle。

**ifNull：**

第一个参数为Query的子类型，为待检测的表达式；<br>
第二个参数为Query的子类型，代表前面的表达式为空时选择的值；<br>
第三个参数为数据库类型。

例子：有些时候我们需要检测sum返回的结果是否是空值，可以使用ifNull函数：

    val select = Select()
                .from("table")
                .select(ifNull(sum("col"), const(0), DB.MYSQL))
                .sql()

生成的sql语句：

    SELECT IFNULL(SUM(col), 0)
    FROM table

注：因为每个数据库的函数差别比较大，所以这种非标准函数需要手动传入想要的数据库类型。

**cast：**

第一个参数为Query的子类型，为待转换的表达式；<br>
第二个参数为String，为想转换的数据库类型。

例子：

    val select = Select().from("table").select(cast(column("a"), "CHAR")).sql()

生成的sql语句：

    SELECT CAST(a AS CHAR)
    FROM table

注：因为各个数据库的类型系统差异较大，如果你的应用需要跨不同的数据库，使用时需要谨慎。

#### 对象映射：
最新版本添加了完整的对象映射功能：<br>
在实体类中添加Kotlin伴生对象，并继承TableSchema类，例如：
    
    data class User(val id: Long? = 1, val name: String? = null, val gender: Int? = 1) {
        companion object : TableSchema("user") {
            val id = column("id")
            val name = column("user_name")
            val gender = column("gender")
        }
    }
    
即可使用对象映射查询：

    val select = Select()
                .from(User)
                .select(User.id + 1)
                .where((User.name like "%xxx%") or (User.name inList listOf("a", "b")) and (User.id gt 1))
                .orderByAsc(User.id)
                .sql()

生成的sql语句：

    SELECT user.id + 1
    FROM user
    WHERE (user.user_name LIKE '%xxx%'
    		OR user.user_name IN ('a', 'b'))
    	AND user.id > 1
    ORDER BY user.id ASC
    
join、子查询等功能也完整支持对象映射特性。<br>

#### 实验性特性：

以下为一些函数和Json操作，目前还只支持mysql和pgsql。

**获取Json：**

使用json（数据库的->操作符）和jsonText(数据库的->>操作符)函数来获取Json数据（支持使用Int下标或者String对象名获取）：

    val select = Select()
                .from("table")
                .select(column("json_col").json(0, DB.MYSQL).json("obj").jsonText("id"))
                .sql()

生成的sql语句：

    SELECT json_col ->> '$[0].obj.id'
    FROM table

pgsql中使用：

    val select = Select(DB.PGSQL)
                .from("table")
                .select(column("json_col").json(0, DB.PGSQL).json("obj").jsonText("id"))
                .sql()

生成的sql语句：

    SELECT CAST(json_col AS JSONB) -> 0 -> 'obj' ->> 'id'
    FROM table

注：<br>
1.在调用链的最初需要指定数据库类型。<br>
2.mysql最终生成的操作符取决于调用链的最后一次操作。

**stringAgg：**<br>
第一个参数为Query的子类型，为需要聚合的表达式；<br>
第二个参数为String，为分隔符；<br>
第三个参数为数据库类型；<br>
第四个参数为可选参数，为聚合函数中的OrderBy子句，默认为空，可以使用Query的扩展函数orderByAsc或orderByDesc构建；<br>
第五个参数为可选参数，Boolean类型，为是否使用DISTINCT，默认为false。

例子：

    val select = Select()
                .from("table")
                .select(stringAgg(column("a"), ",", DB.MYSQL, column("a").orderByAsc(), true))
                .sql()

生成的sql语句：

    SELECT GROUP_CONCAT(DISTINCT a ORDER BY a ASC SEPARATOR ',')
    FROM table

pgsql中使用：

    val select = Select(DB.PGSQL)
                .from("table")
                .select(stringAgg(column("a"), ",", DB.PGSQL, column("a").orderByAsc(), true))
                .sql()

生成的sql语句：

    SELECT STRING_AGG(DISTINCT CAST(a AS VARCHAR), ',' ORDER BY a ASC)
    FROM table


**arrayAgg：**<br>
使用方式同上，在pgsql中生成的sql为ARRAY_TO_STRING(ARRAY_AGG())形式。

**jsonLength：**<br>
第一个参数为Json调用链；<br>
第二个参数为数据库类型。

例子：

    val select = Select()
                .from("table")
                .select(jsonLength(column("json_col").json(0, DB.MYSQL).json("objs"), DB.MYSQL))
                .sql()

生成的sql语句：

    SELECT JSON_LENGTH(json_col, '$[0].objs')
    FROM table

pgsql中使用：

    val select = Select(DB.PGSQL)
                .from("table")
                .select(jsonLength(column("json_col").json(0, DB.PGSQL).json("objs"), DB.PGSQL))
                .sql()

生成的sql语句：

    SELECT JSONB_ARRAY_LENGTH(CAST(json_col AS JSONB) -> 0 -> 'objs')
    FROM table

**findInSet：**<br>
第一个参数为Query的子类型或者String，为需要查询的表达式；<br>
第二个参数为Query的子类型，为需要查询的集合；<br>
第三个参数为数据库类型。

例子：

    val select = Select().from("table").where(findInSet("1", column("a"), DB.MYSQL)).sql()

生成的sql语句：

    SELECT *
    FROM table
    WHERE FIND_IN_SET('1', a)

pgsql中使用：

    val select = Select(DB.PGSQL).from("table").where(findInSet("1", column("a"), DB.PGSQL)).sql()

生成的sql语句：

    SELECT *
    FROM table
    WHERE CAST('1' AS VARCHAR) = ANY(STRING_TO_ARRAY(a, ','))

## 结语：
**此项目旨在为开发者提供一个流畅的sql构建工具，希望能帮助到使用此项目的开发者。**<br>
**后续可能会添加对象映射特性和更多数据库通用功能支持。**<br>
**子查询、join、函数等，在使用时需要慎重，希望大家能写出高质量的sql。**<br>
**文档中没有涉及到的特性等待大家发现。**

## 联系方式：
    微信：wangzhang7982
    邮箱：106497982@qq.com
