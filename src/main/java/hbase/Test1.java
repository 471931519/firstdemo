package hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class Test1 {
    private Connection connection = null;

    @Before
    public void creat() {
        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", "node01,node02,node03");
        try {
            connection = ConnectionFactory.createConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test1() throws IOException {
//        Configuration config = HBaseConfiguration.create();
//        Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("user2");
        if (!admin.tableExists(tableName)) {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            hTableDescriptor.addFamily(new HColumnDescriptor("base_info"));
            admin.createTable(hTableDescriptor);
            System.out.println("建表了");
        }
        System.out.println("有了");
    }

    @Test
    public void test2() throws Exception {
        TableName tableName = TableName.valueOf("user");
        Table table = connection.getTable(tableName);
        HTableDescriptor tableDescriptor = table.getTableDescriptor();
        HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
        for (HColumnDescriptor columnFamily : columnFamilies) {
            byte[] name = columnFamily.getName();
            String s = Bytes.toString(name);
            System.out.println(s);
        }
    }

    @Test
    public void test03() throws Exception {
        TableName tableName = TableName.valueOf("user");
        Table table = connection.getTable(tableName);
        String rowKey = "rowkey_10";
        Put zhangsan = new Put(Bytes.toBytes(rowKey));
        zhangsan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("张三"));
        zhangsan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("20"));
        table.put(zhangsan);
        table.close();
    }

    @Test
    public void test4() throws Exception {
        TableName tableName = TableName.valueOf("user");
        Table table = connection.getTable(tableName);
        String rowKey = "rk0001";
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            System.out.println(Bytes.toString(CellUtil.cloneRow(cell))
                    + "==> " + Bytes.toString(CellUtil.cloneFamily(cell))
                    + "{" + Bytes.toString(CellUtil.cloneQualifier(cell))
                    + ":" + Bytes.toString(CellUtil.cloneValue(cell)) + "}");
        }
    }

    @Test
    public void test5()throws  Exception{
        TableName tableName = TableName.valueOf("user");
        Table table = connection.getTable(tableName);
        Scan scan=new Scan();
//        scan.setStartRow(Bytes.toBytes("rowkey_1"));
//        scan.setStopRow(Bytes.toBytes("rowkey_2"));
//        Filter filter1 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("rk002")));
        Filter filter1 = new FamilyFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes("2")));
        scan.setFilter(filter1);

        ResultScanner scanner = table.getScanner(scan);
        Result next=null;
        while ((next = scanner.next())!=null){
           List<Cell> cells = next.listCells();
           for (Cell cell : cells) {
               System.out.println(Bytes.toString(CellUtil.cloneRow(cell))
                       + "==> " + Bytes.toString(CellUtil.cloneFamily(cell))
                       + "{" + Bytes.toString(CellUtil.cloneQualifier(cell))
                       + ":" + Bytes.toString(CellUtil.cloneValue(cell)) + "}");
           }
       }
    }
}
