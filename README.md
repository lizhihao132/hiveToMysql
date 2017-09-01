# hiveToMysql
hive export to mysql, table field mapping and partly export supported. Finally you can specified constant value for mysql field.

[程序名] HiveToMysql

[作用  ] 从 hive 导出数据到 mysql

[描述  ] 由于 sqoop 对以下功能支持不好:
			1.导出-导入字段的映射
			2.只导入部分字段
			3.导出时无法设置常量值给导入字段
			4.支持重复导入/清除重复数据
		故开发此程序,支持以上功能.
[用法  ] 运行 run.sh.

[额外  ] 本程序在处理出错时会返回错误码给上层调用者, 可以兼容 Tss 捕获脚本运行的错误. Tss 中将 wrap.sh 指定为运行脚本即可.

[配置1 ] dump.conf 是总体配置文件.它指明了 hive 和 mysql 的库信息, 以及一些导出-导出配置, 如下:
			1.dump.conf 中 ds 表示要导出大数据某个日期的数据. ds 不设置或设置为 ? 表示取上一个自然日期
			2.一般情况下 dump.conf 变量的配置不需要更改.它们都有默认值.
			3.delete_before_dump(默认为 false) 若设置为 true 则先从 mysql 库删除数据
				(where 条件参考的是 map 文件中配置的常量,见 map 配置),一般应该这样做,这样
				可以支持重复导入. 但是如果程序的逻辑不支持这样做，应该设置为 false.
			4.skip_dump_to_local_file 跳过导出 hive 数据到本地,(以 skip 开头的参数主要用于测试)
			5.skip_local_file_to_sql 跳过从本地 hive 数据(即上一步的输出)生成 sql 文件
			6.skip_sql_to_target 跳过从 sql 文件(上一步的输出)导入到目标库
			7.error_if_none_data 如果 hive 没有满足条件的数据是否应该中止处理(error: 9998)
			8.error_if_src_field_not_exsits 如果 map 中指定的 hive 字段并不存在是否应中止处理(error: 9997)
			9.返回给上层调用者的错误码:
				9995: hive 导出缺失表头
				9996: hive 导出的数据与表头对应不上
				9997: hive 字段不存在(error_if_src_field_not_exsits=true 时生效)
				9998: 没有数据(error_if_none_data=true 时生效)
				9999: 其它错误

[配置2 ] dump.conf 配置了hive 表到 mysql 表的字段映射. 
			1.配置的格式为 mysql_field=hive_field. 
			2.为了支持导入 mysql 时给字段设置常量,可以给等式右边赋为以 $ 或 # 开头的量.$ 表示后面的变量在
				dump.conf 中配置了, # 表示立即数
			3.只有配置在该文件中的 mysql 字段会被导入值, 没有配置的字段被忽略.这允许了部分字段导出-导入.
			4.配置在该文件中的常量($ 或 # 引导的字段和值)将被用于导入 mysql 前删除.
