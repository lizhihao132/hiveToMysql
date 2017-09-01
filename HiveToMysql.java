import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;

public class HiveToMysql {
    
    private Properties appConf;
    //file name
    private String confFile;
    private String sqlFileName;
    private String dataFileName;
	private String mapFileName;
    
    //hive config
    private String hive_db;
    private String hive_table;
    private String ds;
    private String ds_formater;
    
    //mysql config
    private String mysql_db;
    private String mysql_username;
    private String mysql_password;
    private String mysql_ip;
    private String mysql_port;
    private String mysql_table;
    
    //dump control 
    private Boolean deleteBeforeInsert; 
    
    private Boolean testMode;
    private Boolean skipDumpToLocalFile;
    private Boolean skipLocalFileToSql;
    private Boolean skipSqlToTarget;
    private Boolean errorIfSrcFieldNotExsits;
    private Boolean errorIfNoneData;
    
    //middle data
    private List<String> targetFieldNameList;
    private Map<String, String> fieldNameMap;   //导入字段与导出字段的映射 mysql -> hive
    private Map<String, Integer> srcFieldOrderMap;
    private Map<String,String> constantFieldAndValue; //map 中以 # 开头的是常量, 如 key=$value, 此时 key 字段的值是配置在 conf 中的常量, 以 #开头表示是立即数
    private String dataHeader;  //data 文件的第一行. 即 hive 的表头
    
    private Set<String> targetFieldHasIgnored = new HashSet<String>();
    
    /**
     * 进程需要明确地返回执行结果. 通过 System.exit 来返回. 0 表示正常，其它失败
     * Tss 会读取进程返回码确定调度任务成功或失败
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, InterruptedException
    {
        try{
            String confFile = "dump.conf";
            if(null != args && args.length > 0) 
                confFile = args[0];
            
            HiveToMysql HiveToMysql = new HiveToMysql(confFile);
            int code = HiveToMysql.dumpToTarget();
            
            System.exit(code);
        }
        catch(Throwable th)
        {
            th.printStackTrace();
            System.exit(9999);
        }
    }
    
    public HiveToMysql(String conf) throws IOException
    {
        confFile = conf;
        init();
        initMap();
    }
    
    private void init() throws IOException
    {
        Properties p = new Properties();
        appConf = p;
        p.load(new FileInputStream(confFile));
        
        sqlFileName = p.getProperty("dump_sql_file","dump.sql");
        dataFileName = p.getProperty("dump_mid_file","dump.data");
        mapFileName = p.getProperty("dump_map_file","dump.map");
        
        
        hive_db = p.getProperty("hive_db");
        hive_table = p.getProperty("hive_table");
        ds_formater = p.getProperty("ds_formater","yyyyMMdd");
        Calendar c = Calendar.getInstance();
        c.add(Calendar.DAY_OF_YEAR, -1);
        ds = p.getProperty("ds");
		if(null == ds || "?".equals(ds))
			ds = new SimpleDateFormat(ds_formater).format(c.getTime());
        
        mysql_db = p.getProperty("mysql_db");
        mysql_ip = p.getProperty("mysql_ip");
        mysql_port = p.getProperty("mysql_port");
        mysql_username = p.getProperty("mysql_username");
        mysql_password = p.getProperty("mysql_password");
        mysql_table = p.getProperty("mysql_table");
        
        String delSup = p.getProperty("delete_before_dump","false");
        deleteBeforeInsert = Boolean.valueOf(delSup); 
        
        testMode = "test".equals(p.getProperty("mode","product"));
        skipDumpToLocalFile = "true".equals(p.getProperty("skip_dump_to_local_file","false"));
        skipLocalFileToSql = "true".equals(p.getProperty("skip_local_file_to_sql","false"));
        skipSqlToTarget = "true".equals(p.getProperty("skip_sql_to_target","false"));
        errorIfNoneData = "true".equals(p.getProperty("error_if_none_data","true"));
        errorIfSrcFieldNotExsits = "true".equals(p.getProperty("error_if_src_field_not_exsits","true"));
    }
    
    
    /**
     * 键目标字段，值为来源字段
     * 
     * @throws IOException
     */
    private void initMap() throws IOException
    {
        targetFieldNameList = new ArrayList<>();
        fieldNameMap = new HashMap<>();
        constantFieldAndValue = new HashMap<String,String>();
        
        Properties p = new Properties();
        p.load(new FileInputStream(new File(mapFileName)));
        Set<Entry<Object,Object>> entrys = p.entrySet();
        for(Entry e : entrys)
        {
            String targetFieldName = (String)e.getKey();
            String srcFieldName = (String)e.getValue();
            fieldNameMap.put(targetFieldName, srcFieldName);
            targetFieldNameList.add(targetFieldName);
            
            if('$' == srcFieldName.charAt(0))
            {
                constantFieldAndValue.put(targetFieldName,getValueInConf(srcFieldName.substring(1)));
            }
            else if('#' == srcFieldName.charAt(0))
            {
                constantFieldAndValue.put(targetFieldName, srcFieldName.substring(1));
            }
            else{}
        }
        
        
    }
    
	/**
		取得定义在 conf 中的变量. 注意 ? 是一个特殊符号
	*/
    private String getValueInConf(String var)
    {
        if("ds".equals(var))
        {
            if(null == ds || "?".equals(ds))
            {
                Calendar c = Calendar.getInstance();
                ds = new SimpleDateFormat(ds_formater).format(c.getTime());
            }
            return ds;
        }
        return appConf.getProperty(var);
    }
    
	/**
		导入到目标表中
	*/
    public int dumpToTarget()throws IOException, InterruptedException
    {
        if(!skipDumpToLocalFile)
        {
            int code = dumpToLocal();
            if(0 != code)
                return code;
        }
        if(!skipLocalFileToSql && !localDumpToSql()) //没有数据
        {
            System.out.println("\n---------------------nothing to dump, check data!!!--------------------------\n");
			if(errorIfNoneData)
				return 9998;   
			else
				return 0;
        }
        if(!skipSqlToTarget)
        {
            int code = sqlDumpToTarget();
            if(0 != code)
                return code;
        }
        
        return 0;
    }
    
	/**
		导出到本地
	*/
    private int dumpToLocal() throws IOException, InterruptedException
    {
        FileOutputStream fos = new FileOutputStream(dataFileName,false);
        fos.write("".getBytes());
        fos.close();
	
        String cmd = String.format("hive -e 'select * from %s.%s where ds=%s'>>%s", hive_db,hive_table, ds,dataFileName);
        return execCmd(cmd);
    }
    
	/**
		由本地数据得到导入的 sql
	*/
    private boolean localDumpToSql() throws IOException
    {
        parseDataFile();
        OutputStreamWriter os = new OutputStreamWriter(new FileOutputStream(new File(sqlFileName), false),"utf-8");
        String timeStamp = System.currentTimeMillis()+"";
        String __inner_version_no = "#data timestamp: " + timeStamp + "\n\n";
        os.write(__inner_version_no);
        String sql = getInsertSql();
        if(null == sql || 0 == sql.length())
        {
            os.close();
            return false;
        }
        os.write(sql);
        os.close();
        System.out.println("data timestamp: " + timeStamp);
        return true;
    }
    
	/**
		sql 导出到目标表中
	*/
    private int sqlDumpToTarget() throws IOException, InterruptedException 
    {
        String mysql = String.format("mysql -h%s -P%s -u%s -p%s -D%s --default-character-set=utf8", 
                mysql_ip,mysql_port,mysql_username,mysql_password,mysql_db);
        
        String tmpSh = "";
        int dotPos = confFile.indexOf(".");
        if(-1 == dotPos)
            tmpSh += "_tmp.sh";
        else
            tmpSh += (confFile.substring(0, dotPos) + "_tmp.sh");
         
        FileOutputStream fos = new FileOutputStream(tmpSh, false);
        
        if(deleteBeforeInsert && null != constantFieldAndValue && !constantFieldAndValue.isEmpty())
        {
            StringBuilder deleteWhere = new StringBuilder("where 1=1");
            Set<Entry<String, String>> entrys = constantFieldAndValue.entrySet();
            for(Entry<String, String> entry: entrys)
            {
                deleteWhere.append(" and ").append(entry.getKey()).append("=").append(safeWrapBy(entry.getValue(),"\""));
            }
            String cmd = String.format("%s -e 'delete from %s.%s %s'", mysql, mysql_db, mysql_table,deleteWhere);
            //execCmd(cmd);
            fos.write(cmd.getBytes());
            fos.write("\n\n".getBytes());
        }
        
        String cmd = String.format("%s %s -t -T --default-character-set=utf8 < %s", mysql, mysql_table,sqlFileName);
        //execCmd(cmd);
        fos.write(cmd.getBytes());
        fos.write("\n\n".getBytes());
        fos.close();
        return execCmd("sh " + tmpSh);
    }
    
	/**
		解析导出到本地的 hive 数据
	*/
    private void parseDataFile()throws IOException
    {
        BufferedReader br = new BufferedReader(new FileReader(dataFileName));
        String line = br.readLine();
        if(null == line || 0 == line.length())
        {
            System.out.println("no header. file has been broken: " + dataFileName);
            System.exit(9995);
        }
        br.close();
        dataHeader = line;
        List<String> srcFieldNameList = getSrcFieldName(line);
        srcFieldOrderMap = fieldNameToSrcOrder(srcFieldNameList);
        
        for(String targetFieldName : targetFieldNameList)
        {
            //如果 targetFieldName 并没有配置常量, 而且变量名
            if(!constantFieldAndValue.containsKey(targetFieldName))
            {
                String srcFieldName = fieldNameMap.get(targetFieldName);
                Integer order = srcFieldOrderMap.get(srcFieldName);
                if(null == order)
                {
                    targetFieldHasIgnored.add(targetFieldName);
                    System.out.println("no field named '" + srcFieldName + "' in source table. mapping: '" + targetFieldName + "'.");
                }
                else
                {
                    System.out.println(targetFieldName + " -----> " + srcFieldName + ", " + order);
                }
            }
        }
        
        if(!targetFieldHasIgnored.isEmpty() && errorIfSrcFieldNotExsits)
        {
            System.out.println("--------- dump will exit ---------");
            System.exit(9997);
        }
    }
    
	/**
		获得 insert 语句的 sql
	*/
    private String getInsertSql() throws IOException
    {
        StringBuffer ret = new StringBuffer();
        BufferedReader br = new BufferedReader(new FileReader(dataFileName));
        br.readLine();  //读掉头
        String line ;
        int ncount = 0;
        while(null != (line = br.readLine()))
        {
            if(currentLineIsHeader(line))
            {
                continue;
            }
            
            if(0 == ncount % 100)
            {
                if(0 != ncount)
                {
                    ret.append(";\n\n");
                }
                ret.append("insert into ").append(mysql_db).append(".").append(mysql_table);
                ret.append(insertHeader());
                ret.append(" values \n");
            }
            else
            {
                ret.append(",\n");
            }
            String[] values = line.split("\t");
            ret.append(insertValues(values));
            ++ ncount;
        }
        if(ret.length() > 2 && ",\n".equals(ret.substring(ret.length()-2, ret.length())))
        {
            ret.delete(ret.length()-2, ret.length());
        }
        if(0 != ret.length())
            ret.append(";");
        return ret.toString();
    }
    
	/**
		获得 insert 语句的头
	*/
    private String insertHeader()
    {
        StringBuffer ret = new StringBuffer();
        ret.append("(");
        for(String field : targetFieldNameList)
        {
            if(!targetFieldHasIgnored.contains(field))
                ret.append(field).append(",");
        }
        if(0 != ret.length() && ',' == ret.charAt(ret.length()-1))
            ret.deleteCharAt(ret.length()-1);
        ret.append(")");
        return ret.toString();
    }
    
    /**
     * 使用 hive -e 'select * from hive_table where ..' >> result.data 的命令导出的数据的非首行可能含有表头, 或者包含表头
     * @param values
     * @return
     */
    private boolean currentLineIsHeader(String line)
    {
        return dataHeader.equals(line) || -1 != line.indexOf(dataHeader);
    }
    
    /**
	 * 获得 insert 语句的 values
     * 根据配置的字段映射: target=src, 插入 src 字段的值. 注意 map 中的 $变量和 #常量
     * 若 src 字段不在 hive 表中则忽略对应的 target 字段
     * @param srcValues
     * @return
     */
    private String insertValues(String [] srcValues)
    {
        StringBuffer ret = new StringBuffer("");
        ret.append("(");
        for(int i = 0;i < targetFieldNameList.size(); ++ i)
        {
            String targetFieldName = targetFieldNameList.get(i);
            String srcFieldName = fieldNameMap.get(targetFieldName);
            assert(null != srcFieldName);

            if(targetFieldHasIgnored.contains(targetFieldName))
            {
                continue;
            }
            else if(constantFieldAndValue.containsKey(targetFieldName))
            {
                String value = constantFieldAndValue.get(targetFieldName);
                ret.append("'").append(value).append("'");
            }
            else 
            {
                Integer srcIndex = srcFieldOrderMap.get(srcFieldName);
                assert(null!=srcIndex && srcIndex > -1);
                if(srcIndex >= srcValues.length)
                {
                    System.out.println("no value of field '" + srcFieldName + "' in source table. file has been broken.");
                    System.exit(9996);
                }
                String value = srcValues[srcIndex];
                if("NULL".equals(value))
                {
                    ret.append("NULL");
                }
                else
                {
                    ret.append(safeWrap(value));
                }
            }
            ret.append(",");
        }
        if(0 != ret.length())
            ret.deleteCharAt(ret.length() - 1); //删除多作的 ,
        
        ret.append(")");
        return ret.toString();
    }
    
    /**
	 * 简单地防止 sql 注入. 也为了得到正确的 sql 语句
     * 不存在 ' 或 " 则无所谓用什么去包裹
     * 若串中存在 ' 则使用 " 去包裹
     * 若串中存在 " 则使用 ' 去包裹
     * 存在 ' 和 " 则转义成 html 字符
     * @param str
     * @return
     */
    private String safeWrap(String str)
    {
        StringBuffer ret = new StringBuffer();
        if(-1 == str.indexOf('\'') && -1 == str.indexOf('\"'))
        {
            return ret.append("\'").append(str).append("\'").toString();
        }
        else if(-1 != str.indexOf('\'') && -1 == str.indexOf('\"'))
        {
            return ret.append("\"").append(str).append("\"").toString();
        }
        else if(-1 != str.indexOf('\"') && -1 == str.indexOf('\''))
        {
            return ret.append("\'").append(str).append("\'").toString();
        }
        else 
        {
            str.replaceAll("\'", "&#39;");
            str.replaceAll("\"", "&#34;");
            return ret.append("\'").append(str).append("\'").toString();
        }
    }
    
	/**
		使用 wrapStr 去包围 str
	*/
    private String safeWrapBy(String str, String wrapStr)
    {
        StringBuffer ret = new StringBuffer();
        if(-1 == str.indexOf(wrapStr))
        {
            return ret.append(wrapStr).append(str).append(wrapStr).toString();
        }
        else 
        {
            str.replaceAll(wrapStr, "&#34;");
            return ret.append(wrapStr).append(str).append(wrapStr).toString();
        }
    }
    
	/**
		得到源表中的字段在导出结果表头中的顺序
	*/
    private Map<String, Integer> fieldNameToSrcOrder(List<String> fieldName)
    {
        Map<String, Integer> ret = new HashMap<>();
        int order = 0;
        for(String field: fieldName)
        {
            ret.put(field, order ++);
        }
        return ret;
    }
    
    /**
	 * 得到源表导出数据中的表头
     * hive 导到本地的 data 中的第一行是 header，一般为 hive_table.field1  hive_table.field2 ...
     * 此函数取出 field1, field2 ...
     * 兼容表头没  hive_table. 只有 field 的情况
     * @param header
     * @return
     */
    private List<String> getSrcFieldName(String header)
    {
        List<String> fields = new ArrayList<>();
        String []fieldsWithDot = header.split("\t");
        if(null != fieldsWithDot && 0 != fieldsWithDot.length)
        {
            for(String fieldWithDot : fieldsWithDot)
            {
                int dotPos = fieldWithDot.indexOf('.');
                fields.add(fieldWithDot.substring(dotPos+1));
            }
        }
        return fields;
    }
    
	/**
		执行 cmds 命令列表
	*/
    public int execCmd(List<String> cmds) throws IOException, InterruptedException {
        ProcessBuilder pb =new ProcessBuilder(cmds);
        
        return printOutputStream(pb.start(),null);
    }
    
	/**
		执行 cmd 命令
	*/
    public int execCmd(String cmd) throws IOException, InterruptedException {
        if(testMode)
        {
            System.out.println(cmd);
            return 0;
        }
        
        List<String> cmds = new ArrayList<String>();
        cmds.add("sh");
        cmds.add("-c");
        cmds.add(cmd);
        System.out.println("sh -c "+cmd);
        ProcessBuilder pb = new ProcessBuilder(cmds);
        return printOutputStream(pb.start(), null);
    }
    
    public void testCmd()throws IOException, InterruptedException {
        execCmd("ping 123.0.0.1");
    }
    
	/**
		实时打印进程 process 的输出流
	*/
    private int printOutputStream(final Process process, List<String> processOutputs)throws IOException, InterruptedException
    {
        final StringBuilder result []= {new StringBuilder(), new StringBuilder()};
        final BufferedReader readers[]  = {new BufferedReader(new InputStreamReader(process.getInputStream(), "UTF-8")),
                new BufferedReader(new InputStreamReader(process.getErrorStream(), "UTF-8"))};
        
        final PrintStream outPrint[] = {System.out, System.err};
        
        Runnable []rs = new Runnable[readers.length];
        for(int i =0;i < readers.length;++ i)
        {
            final int index = i;
            rs[i] = new Runnable() 
            {
                @Override
                public void run() 
                {
                    BufferedReader bufrIn = readers[index];
                    try 
                    {
                        String line = null;
                        while ((line = bufrIn.readLine()) != null) {
                            result[index].append(line).append('\n');
                            outPrint[index].println(line);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        closeStream(bufrIn);
                    }
                }
                
                private void closeStream(Closeable stream) {
                    if (stream != null) {
                        try {
                            stream.close();
                        } catch (Exception e) {
                            // nothing
                        }
                    }
                }
            };
        }
        for(Runnable r : rs)
        {
            Thread thread = new Thread(r);
            thread.start();
        }
        
        int exitCode = process.waitFor();
        if(null != processOutputs)
        {
            for(final StringBuilder r : result)
            {
                processOutputs.add(r.toString());
            }
        }
        return exitCode;
    }
}
