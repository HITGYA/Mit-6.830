package simpledb.common;

import com.sun.xml.internal.ws.api.model.wsdl.WSDLOutput;
import simpledb.common.Type;
import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
//    catalog由数据库中现存的表的列表和表对应的schema数据组成，
//    我们将实现向catalog中添加table以及获取table信息的功能。与table关联的是一个TupleDesc对象，通过该对象可以确定表字段个数和类型
//    SimpleDB含有一个Catalog的单例，我们可以通过Database.getCatalog()方法获取到该单例对象，可以通过Database.getBufferPool()获取到单例BufferPool对象
public class Catalog {
    public class Table {
        private DbFile file;
        private String tableName;
        private String pkeyField;

        public Table(DbFile file, String tableName, String pkeyField) {
            this.file = file;
            this.tableName = tableName;
            this.pkeyField = pkeyField;
        }
    }
        /**
         * key:DbFile#getId()
         * value:Table Info
         */
        private Map<Integer, Table> tables;

        /**
         * key:table name
         * value:table id
         */
        private Map<String, Integer> nameToId;

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
        public Catalog() {
            // done
            tables = new ConcurrentHashMap<>();
            nameToId = new ConcurrentHashMap<>();
        }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // done
        Table table = new Table(file , name, pkeyField);
        tables.put(file.getId(), table);
        nameToId.put(name, file.getId());
    }
    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }
    //随机字符串作为name参数

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // done
        if (name == null || !nameToId.containsKey(name)) {
            throw new NoSuchElementException("A table named " + name + " does not exist");
        }
        return nameToId.get(name);
    }

      /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // done
        if(tables.containsKey(tableid) ) {
         Table table = tables.get(tableid);
         return table.file.getTupleDesc();
        }
        throw new NoSuchElementException("Table " + tableid + " does not exist");
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // done
        if(tables.containsKey(tableid)){
            Table table =tables.get(tableid);
            return table.file;
        }
        throw new NoSuchElementException("Table " + tableid + " does not exist");
    }

    public String getPrimaryKey(int tableid) {
        // done
        if (tables.containsKey(tableid)) {
            return tables.get(tableid).pkeyField;
        }
        return null;
    }

    public Iterator<Integer> tableIdIterator() {
        // done
        return tables.keySet().iterator();
    }

    public String getTableName(int id) {
        // done
        if(tables.containsKey(id)){
            Table table = tables.get(id);
            return table.tableName;
        }
        return null;
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        // done
        this.tables.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().equalsIgnoreCase("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}
//       1 从指定的 catalogFile 中读取文件路径。
//       2 创建一个 BufferedReader 对象来逐行读取文件内容。
//       3 使用 while 循环来逐行解析文件内容。
//       4 对于每一行，首先提取表名，即行中括号前的部分。
//       5 提取字段和数据类型的部分，即行中括号内的部分，然后将其按逗号进行分割得到字段和类型的数组。
//       6 创建两个空的 ArrayList，分别用于存储字段名和数据类型。
//       7 遍历字段和类型的数组，对于每一个元素，提取字段名和类型，并根据类型创建相应的 Type 对象。如果遇到未知类型或注解，将打印错误信息并退出程序。
//       8 如果字段数组的长度为 3，说明存在主键注解，提取主键字段名。
//       9 将字段名和数据类型转换为数组，并使用它们创建一个 TupleDesc 对象表示表格的模式。
//       10使用表格名称和模式创建一个新的 HeapFile 对象，并传入基于目录和表名的文件路径。
//       11调用 addTable 方法将创建的堆文件、表格名称和主键字段信息添加到表格管理器中。
//       12打印成功添加的表格名称和模式信息。

