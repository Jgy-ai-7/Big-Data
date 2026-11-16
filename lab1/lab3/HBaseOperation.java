import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseOperation {

    private static Configuration conf;
    private static Connection conn;

    public static void init() throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conn = ConnectionFactory.createConnection(conf);
    }

    public static void createTable(String tableName, String[] fields) throws IOException {
        Admin admin = conn.getAdmin();
        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            if (admin.isTableEnabled(tn)) {
                admin.disableTable(tn);
            }
            admin.deleteTable(tn);
            System.out.println("已删除原有表: " + tableName);
        }
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tn);
        for (String f : fields) {
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(f)).build());
        }
        admin.createTable(builder.build());
        admin.close();
        System.out.println("创建表成功: " + tableName);
    }

    public static void addRecord(String tableName, String row, String[] fields, String[] values) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(row));
        for (int i = 0; i < fields.length; i++) {
            String[] cfq = fields[i].split(":");
            if (cfq.length == 2) {
                put.addColumn(Bytes.toBytes(cfq[0]), Bytes.toBytes(cfq[1]), Bytes.toBytes(values[i]));
            } else {
                put.addColumn(Bytes.toBytes(fields[i]), Bytes.toBytes(""), Bytes.toBytes(values[i]));
            }
        }
        table.put(put);
        table.close();
        System.out.println("添加记录成功: " + row);
    }

    public static void scanColumn(String tableName, String column) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        String[] cfq = column.split(":");
        if (cfq.length == 2) {
            scan.addColumn(Bytes.toBytes(cfq[0]), Bytes.toBytes(cfq[1]));
        } else {
            scan.addFamily(Bytes.toBytes(column));
        }
        ResultScanner scanner = table.getScanner(scan);
        for (Result r : scanner) {
            String rowKey = Bytes.toString(r.getRow());
            System.out.println("行键: " + rowKey);
            Cell[] cells = r.rawCells();
            if (cells.length == 0) {
                System.out.println("  null");
            }
            for (Cell cell : cells) {
                String fam = Bytes.toString(CellUtil.cloneFamily(cell));
                String qual = Bytes.toString(CellUtil.cloneQualifier(cell));
                String val = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println("  " + fam + ":" + qual + " = " + val);
            }
        }
        scanner.close();
        table.close();
    }

    public static void modifyData(String tableName, String row, String column, String newValue) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        String[] cfq = column.split(":");
        if (cfq.length != 2) {
            System.out.println("column 格式应为 列族:列限定符");
            table.close();
            return;
        }
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(cfq[0]), Bytes.toBytes(cfq[1]), Bytes.toBytes(newValue));
        table.put(put);
        table.close();
        System.out.println("修改完成: " + row + " " + column + " = " + newValue);
    }

    public static void deleteRow(String tableName, String row) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Delete del = new Delete(Bytes.toBytes(row));
        table.delete(del);
        table.close();
        System.out.println("删除完成: " + row);
    }

    public static void addColumnFamily(String tableName, String family) throws IOException {
        Admin admin = conn.getAdmin();
        TableName tn = TableName.valueOf(tableName);
        admin.addColumnFamily(tn, ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build());
        admin.close();
        System.out.println("已添加列族: " + family);
    }

    public static void deleteColumnFamily(String tableName, String family) throws IOException {
        Admin admin = conn.getAdmin();
        TableName tn = TableName.valueOf(tableName);
        admin.deleteColumnFamily(tn, Bytes.toBytes(family));
        admin.close();
        System.out.println("已删除列族: " + family);
    }

    public static long countRows(String tableName) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        long count = 0;
        for (Result r : scanner) {
            count++;
        }
        scanner.close();
        table.close();
        return count;
    }

    public static void main(String[] args) throws Exception {
        init();

        createTable("Student", new String[]{"info"});

        addRecord("Student", "2015001",
                new String[]{"info:S_Name", "info:S_Sex", "info:S_Age"},
                new String[]{"Zhangsan", "male", "23"});
        addRecord("Student", "2015002",
                new String[]{"info:S_Name", "info:S_Sex", "info:S_Age"},
                new String[]{"Mary", "female", "22"});
        addRecord("Student", "2015003",
                new String[]{"info:S_Name", "info:S_Sex", "info:S_Age"},
                new String[]{"Lisi", "male", "24"});

        System.out.println("---- 浏览 info 列族全部数据 ----");
        scanColumn("Student", "info");

        System.out.println("---- 浏览 info:S_Name 列 ----");
        scanColumn("Student", "info:S_Name");

        modifyData("Student", "2015001", "info:S_Age", "25");
        System.out.println("---- 修改后 ----");
        scanColumn("Student", "info:S_Age");

        System.out.println("表 Student 行数: " + countRows("Student"));

        createTable("SC", new String[]{"Score"});
        addRecord("SC", "2015001",
                new String[]{"Score:Math", "Score:English"},
                new String[]{"86", "69"});
        System.out.println("---- SC 表 ----");
        scanColumn("SC", "Score");

        deleteRow("SC", "2015001");
        System.out.println("SC 表行数: " + countRows("SC"));

        conn.close();
    }
}
