## 项目介绍
1.本项目是一个使用Kotlin语言编写，基于阿里druid项目的sql解析模块构建的sql构建工具，可以使用链式调用构建较为复杂的跨数据库sql语句（目前支持mysql、pgsql、oracle、hive）。<br><br>
2.本项目十分轻量级，除了kotlin基础库和druid支持外，没有任何库引用。<br><br>
3.本项目的初衷是希望能为开发者提供一个贴近原生sql写法的跨数据库sql生成器，这一点在项目需要对接多种数据库时非常有用。<br><br>
4.因为数据库增删改查语句中，SELECT语句最为复杂，所以此项目重点为SELECT语句的拼装。<br><br>
5.完整支持对象映射特性。但也支持使用非对象映射方式构建，因为实际开发中经常有表名、表结构非固定的情况。<br><br>
6.**在此感谢温绍锦先生和他的团队开发出了如此优秀的跨数据库sql parser。**
## 快速开始
#### 来看一个构建简单sql的例子：

在实体类中添加Kotlin伴生对象，并继承TableSchema类，例如：

    data class User(val id: Long? = 1, val name: String? = null, val gender: Int? = 1) {
        companion object : TableSchema("user") {
            val id = column("id")
            val name = column("user_name")
            val gender = column("gender")
        }
    }

使用链式调用：

    val select = Select().from(User).sql()

生成的sql语句：

    SELECT *
    FROM user

注：<br>
from函数第一个参数接收一个TableSchema的子类或者一个字符串，第二个参数为别名，可不传。

#### 支持筛选列：

    val select = Select().from(User).select(User.id, User.name alias "name").sql()

生成的sql语句：

    SELECT user.id, user.user_name AS name
    FROM user

注：<br>
1.select可以接收若干个字符串、用column包裹的对象属性，也可以接收若干个复杂查询（后文会介绍）。<br>
2.此处的alias是对String类和Query类（内置查询类型的父类）实现的扩展函数，使用了Kotlin的扩展函数和中缀函数语法，用来起别名（数据库的AS子句）。<br>
3.此处的User.id，在Java中要使用User.Companion.getId()的方式获取。

#### 当然也支持SELECT语句的各种功能：

    val select = Select()
                .from(User)
                .select(User.id alias "c1", User.name alias "c2")
                .where(User.id eq 1)
                .orderByAsc(User.name)
                .limit(10, 100)
                .sql()

生成的sql语句：

    SELECT user.id AS c1, user.user_name AS c2
    FROM user
    WHERE user.id = 1
    ORDER BY user.user_name ASC
    LIMIT 100, 10

注：<br>
1.where中使用链式调用的语法，后文会介绍。<br>
2.提供orderByAsc和orderByDesc两个函数排序。

#### 跨数据库支持：
创建Select对象的时候可以指定数据库类型（默认为mysql）：

    val select = Select(DB.PGSQL)
                .from(User)
                .limit(10, 100)
                .sql()

生成的sql语句：

    SELECT *
    FROM user
    LIMIT 10 OFFSET 100

#### 聚合函数：

    val select = Select()
                .from(User)
                .select(User.name, count())
                .groupBy(User.name)
                .sql()

生成的sql语句：

    SELECT user.user_name, COUNT(*)
    FROM user
    GROUP BY user.user_name

支持的聚合函数有count、countDistinct、sum、avg、max、min。

#### 二元操作符：
可以使用二元操作符构建复杂的sql语句：

    val select = Select()
                .from(User)
                .select((User.id + User.gender) / 2 alias "col")
                .where((User.id eq 1) and (User.gender eq 2))
                .sql()

生成的sql语句：

    SELECT (user.id + user.gender) / 2 AS col
    FROM user
    WHERE user.id = 1
    AND user.gender = 2

注：<br>
1.计算用的二元操作符（+、-、*、/、%）使用Kotlin的运算符重载，因为Java中不支持此功能，所以Java调用的时候需要使用.plus()等方式。<br>

2.如果不使用对象映射，列名使用column()函数包装；常量使用const()函数（Java调用使用value()函数），当然大多数情况下const()都可以省略。列、常量、聚合函数等类型因为继承了公共父类Query，所以均可使用二元运算符自由组合。<br>

3.支持以下逻辑运算符：eq(=)、ne(!=)、gt(>)、ge(>=)、lt(<)、le(<=)、and(AND)、or(OR)、xor(XOR)。<br>

4.因为Kotlin不支持自定义运算符的优先级，所以在使用and、or等符号，或者运算符的右侧为多个非const()包装的常量时，每一组运算都需要放在括号中（如果只是and条件，可以用多个where函数调用的方式）。<br>

5.Java调用中缀函数时，需要import static对应的dsl函数，并且把中缀调用改为前缀调用，比如 a eq b，需要写成eq(a, b)。<br>

#### 其他操作符：
支持inList(IN)、notInList(NOT IN)、like(LIKE)、notLike(NOT LIKE)、isNull(IS NULL)、isNotNull(IS NOT NULL)、between(BETWEEN)。

    val select = Select()
                .from(User)
                .where(User.gender inList listOf(1, 2))
                .where(User.id between (1 to 10))
                .where(User.name.isNotNull())
                .where(User.name like "%xxx%")
                .sql()

生成的sql语句：

    SELECT *
    FROM user
    WHERE user.gender IN (1, 2)
    	AND user.id BETWEEN 1 AND 10
    	AND user.user_name IS NOT NULL
    	AND user.user_name LIKE '%xxx%'
        
注：between函数中缀调用时，接受一个Pair二元组，两个值使用Kotlin的中缀函数to隔开，Java调用时可以使用between(query, start, end)的方式。

#### 有条件的WHERE子句：
有时候我们需要动态拼接条件，比如检验某个传入的参数不为空时才拼接，例如：

    val userName: String? = null // 假设此处为用户传参
    val select = Select()
                .from(User)
                .where({ !userName.isNullOrEmpty() }, User.name eq userName)
                .sql()

生成的sql语句：

    SELECT *
    FROM user

此处的where是一个高阶函数，第一个参数为一个返回值为Boolean的函数，当然也可以省略大括号，传入一个Boolean表达式。

#### JOIN子句：
支持的join类型有：join、innerJoin、leftJoin、rightJoin、crossJoin、fullJoin等。

    val select = Select()
                .from(User)
                .leftJoin(User1,on = User.id eq User1.id)
                .sql()

生成的sql语句：

    SELECT *
    FROM user
        LEFT JOIN user1 ON user.id = user1.id

#### CASE WHEN子句：
使用case()函数和中缀函数then与elseIs构建一个CASE WHEN子句：

    val select = Select()
                .from(User)
                .select(case(User.gender eq 1 then "男", User.gender eq 2 then "女") elseIs "其他" alias "gender")
                .sql()

生成的sql语句：

    SELECT CASE 
		    WHEN user.gender = 1 THEN '男'
		    WHEN user.gender = 2 THEN '女'
		    ELSE '其他'
	    END AS gender
    FROM user

当然case()函数也可以传入count()和sum()中：

    val case = case(User.gender eq 1 then User.gender) elseIs null
    val select = Select()
                .from(User)
                .select(count(case) alias "male_count")
                .sql()

生成的sql语句：

    SELECT COUNT(CASE 
		    WHEN user.gender = 1 THEN user.gender
		    ELSE NULL
	    END) AS male_count
    FROM user

#### UNION和UNION ALL：

    val select = (Select().from(User).select(User.id) union
                Select().from(User).select(User.id) unionAll
                Select().from(User).select(User.id)).sql()

生成的sql语句：

    SELECT user.id
    FROM user
    UNION
    SELECT user.id
    FROM user
    UNION ALL
    SELECT user.id
    FROM user

注：union右侧创建的Select对象的数据库类型取决于union左侧的Select对象。

#### 子查询：
支持from、join、各种操作符的**右侧**使用子查询：

**from中的子查询：**

    val select = Select().from(Select().from(User)).sql()

生成的sql语句：

    SELECT *
    FROM (
	    SELECT 
	    FROM user
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

注：join中的子查询因为Kotlin语法限制，如果需要起别名，不要使用对象映射。

**操作符右侧的子查询：**

    val select = Select()
                .from(User)
                .select(User.id inList Select().from(User).select(User.id).limit(10))
                .sql()

生成的sql语句：

    SELECT user.id IN (
		    SELECT user.id
		    FROM user
		    LIMIT 0, 10
	    )
    FROM user

#### 常用数据库函数：

**concat和concatWs：**

用于字符串拼接。<br>
concat是一个可变长参数的函数，接收的参数为各种字段、常量等Query的子类型。<br>
concatWs的第一个参数为分隔符的字符串，其他同concat。

    val select = Select()
                .from(User)
                .select(concat(User.id, const(","), User.name))
                .select(concatWs(",", User.id, User.name))
                .sql()

生成的sql语句：

    SELECT CONCAT(user.id, ',', user.user_name)
        , CONCAT_WS(',', user.id, user.user_name)
    FROM user

注：以上函数暂不支持oracle。

**ifNull：**

用于表达式为空时赋予默认值。<br>
第一个参数为Query的子类型，为待检测的表达式；<br>
第二个参数为Query的子类型，代表前面的表达式为空时选择的值。<br>

例子：有些时候我们需要检测sum返回的结果是否是空值，可以使用ifNull函数：

    val select = Select()
                .from(User)
                .select(ifNull(sum(User.age), 0))
                .sql()

生成的sql语句：

    SELECT IFNULL(SUM(user.age), 0)
    FROM user

注：因为每个数据库的函数差别比较大，所以转换出来的函数都不相同。

**cast：**

用于数据库类型转换。<br>
第一个参数为Query的子类型，为待转换的表达式；<br>
第二个参数为String，为想转换的数据类型。

例子：

    val select = Select().from(User).select(cast(User.id, "CHAR")).sql()

生成的sql语句：

    SELECT CAST(user.id AS CHAR)
    FROM user

注：因为各个数据库的类型系统差异较大，如果你的应用需要跨不同的数据库，使用时需要谨慎。

#### 实验性特性：

**以下特性目前还只支持mysql和pgsql。**<br>

以下为一些函数和Json操作。

**获取Json：**

使用json（数据库的->操作符）和jsonText(数据库的->>操作符)函数来获取Json数据（支持使用Int下标或者String对象名获取）：

    val select = Select()
                .from(User)
                .select(User.jsonInfo.json(0).json("obj").jsonText("id"))
                .sql()

生成的sql语句：

    SELECT user.json_info ->> '$[0].obj.id'
    FROM user

pgsql中使用：

    val select = Select(DB.PGSQL)
                .from(User)
                .select(User.jsonInfo.json(0).json("obj").jsonText("id"))
                .sql()

生成的sql语句：

    SELECT CAST(user.json_info AS JSONB) -> 0 -> 'obj' ->> 'id'
    FROM user

注：mysql最终生成的操作符取决于调用链的最后一次操作。

**stringAgg：**<br>

用于字符串聚合。<br>
第一个参数为Query的子类型，为需要聚合的表达式；<br>
第二个参数为String，为分隔符；<br>
第三个参数为可选参数，为聚合函数中的OrderBy子句，默认为空，使用函数orderByAsc或orderByDesc构建；<br>
第四个参数为可选参数，Boolean类型，为是否使用DISTINCT，默认为false。

例子：

    val select = Select()
                .from(User)
                .select(stringAgg(User.name, ",", orderByAsc(User.id).orderByDesc(User.gender), true))
                .sql()

生成的sql语句：

    SELECT GROUP_CONCAT(DISTINCT user.user_name ORDER BY user.id ASC, user.gender DESC SEPARATOR ',')
    FROM user

pgsql中使用：

    val select = Select(DB.PGSQL)
                .from(User)
                .select(stringAgg(User.name, ",", orderByAsc(User.id).orderByDesc(User.gender), true))
                .sql()

生成的sql语句：

    SELECT STRING_AGG(DISTINCT CAST(user.user_name AS VARCHAR), ',' ORDER BY user.id ASC, user.gender DESC)
    FROM user


**arrayAgg：**<br>
使用方式同上，在pgsql中生成的sql为ARRAY_TO_STRING(ARRAY_AGG())形式。

**jsonLength：**<br>

作用为获取json的数组长度。<br>
参数为Json调用链或一个Query的子类型。<br>

例子：

    val select = Select()
                .from(User)
                .select(jsonLength(User.jsonInfo.json(0).json("objs")))
                .sql()

生成的sql语句：

    SELECT JSON_LENGTH(user.json_info, '$[0].objs')
    FROM user

pgsql中使用：

    val select = Select(DB.PGSQL)
                .from(User)
                .select(jsonLength(User.jsonInfo.json(0).json("objs")))
                .sql()

生成的sql语句：

    SELECT JSONB_ARRAY_LENGTH(CAST(user.json_info AS JSONB) -> 0 -> 'objs')
    FROM user

**findInSet：**<br>

用于查询元素是否在某个以","隔开的字符串中。<br>
第一个参数为Query的子类型或者String，为需要查询的表达式；<br>
第二个参数为Query的子类型，为需要查询的集合。<br>

例子：

    val select = Select().from(User).where(findInSet("1", User.ids)).sql()

生成的sql语句：

    SELECT *
    FROM user
    WHERE FIND_IN_SET('1', user.ids)

pgsql中使用：

    val select = Select(DB.PGSQL).from(User).where(findInSet("1", User.ids)).sql()

生成的sql语句：

    SELECT *
    FROM user
    WHERE CAST('1' AS VARCHAR) = ANY(STRING_TO_ARRAY(user.ids, ','))

## 结语：
**此项目旨在为开发者提供一个流畅的sql构建工具，希望能帮助到使用此项目的开发者。**<br>
**子查询、join、函数等，在使用时需要慎重，希望大家能写出高质量的sql。**<br>
**文档中没有涉及到的特性等待大家发现。**

## 联系方式：
    微信：wangzhang7982
    邮箱：106497982@qq.com
