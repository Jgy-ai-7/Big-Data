import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.*;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

public class HDFSOperation {
    static FileSystem fs;
    static Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        fs = FileSystem.get(conf);

        while (true) {
            System.out.println("\n===== HDFS 操作菜单 =====");
            System.out.println("1. 上传文件");
            System.out.println("2. 下载文件");
            System.out.println("3. 查看文件内容");
            System.out.println("4. 查看文件信息");
            System.out.println("5. 递归查看目录信息");
            System.out.println("6. 创建/删除文件");
            System.out.println("7. 创建/删除目录");
            System.out.println("8. 追加内容到文件");
            System.out.println("9. 删除文件");
            System.out.println("10. 移动文件");
            System.out.println("0. 退出");
            System.out.print("请选择操作: ");
            int choice = scanner.nextInt();
            scanner.nextLine();

            switch (choice) {
                case 1: uploadFile(); break;
                case 2: downloadFile(); break;
                case 3: catFile(); break;
                case 4: fileInfo(); break;
                case 5: listDirRecursive(); break;
                case 6: createDeleteFile(); break;
                case 7: createDeleteDir(); break;
                case 8: appendFile(); break;
                case 9: deleteFile(); break;
                case 10: moveFile(); break;
                case 0: fs.close(); System.out.println("退出"); return;
                default: System.out.println("无效选择");
            }
        }
    }

    static void uploadFile() throws Exception {
        System.out.print("本地文件路径: ");
        String local = scanner.nextLine();
        System.out.print("HDFS目标路径: ");
        String hdfs = scanner.nextLine();
        Path src = new Path(local);
        Path dst = new Path(hdfs);
        if (fs.exists(dst)) {
            System.out.print("文件已存在，1-追加 2-覆盖: ");
            int c = scanner.nextInt();
            scanner.nextLine();
            if (c == 1) {
                FSDataOutputStream out = fs.append(dst);
                FileInputStream in = new FileInputStream(local);
                byte[] buf = new byte[1024];
                int len;
                while ((len = in.read(buf)) > 0) out.write(buf, 0, len);
                in.close();
                out.close();
                System.out.println("追加完成");
            } else {
                fs.copyFromLocalFile(false, true, src, dst);
                System.out.println("覆盖完成");
            }
        } else {
            fs.copyFromLocalFile(src, dst);
            System.out.println("上传完成");
        }
    }

    static void downloadFile() throws Exception {
        System.out.print("HDFS文件路径: ");
        String hdfs = scanner.nextLine();
        System.out.print("本地目标目录: ");
        String localDir = scanner.nextLine();
        Path src = new Path(hdfs);
        String fileName = src.getName();
        File localFile = new File(localDir, fileName);
        String finalName = fileName;
        int i = 1;
        while (localFile.exists()) {
            finalName = fileName + "_" + i;
            localFile = new File(localDir, finalName);
            i++;
        }
        fs.copyToLocalFile(src, new Path(localFile.getAbsolutePath()));
        System.out.println("下载完成，保存为: " + finalName);
    }

    static void catFile() throws Exception {
        System.out.print("HDFS文件路径: ");
        String hdfs = scanner.nextLine();
        FSDataInputStream in = fs.open(new Path(hdfs));
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line;
        while ((line = reader.readLine()) != null) System.out.println(line);
        reader.close();
    }

    static void fileInfo() throws Exception {
        System.out.print("HDFS文件路径: ");
        String hdfs = scanner.nextLine();
        FileStatus status = fs.getFileStatus(new Path(hdfs));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println("权限: " + status.getPermission());
        System.out.println("大小: " + status.getLen() + " 字节");
        System.out.println("创建时间: " + sdf.format(new Date(status.getModificationTime())));
        System.out.println("路径: " + status.getPath());
    }

    static void listDirRecursive() throws Exception {
        System.out.print("HDFS目录路径: ");
        String hdfs = scanner.nextLine();
        RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path(hdfs), true);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        while (it.hasNext()) {
            LocatedFileStatus f = it.next();
            System.out.println(f.getPermission() + "  " + f.getLen() + "  " +
                sdf.format(new Date(f.getModificationTime())) + "  " + f.getPath());
        }
    }

    static void createDeleteFile() throws Exception {
        System.out.print("HDFS文件路径: ");
        String hdfs = scanner.nextLine();
        Path p = new Path(hdfs);
        System.out.print("1-创建 2-删除: ");
        int c = scanner.nextInt();
        scanner.nextLine();
        if (c == 1) {
            if (!fs.exists(p.getParent())) fs.mkdirs(p.getParent());
            if (!fs.exists(p)) {
                FSDataOutputStream out = fs.create(p);
                out.close();
                System.out.println("文件创建成功");
            } else System.out.println("文件已存在");
        } else {
            if (fs.exists(p)) { fs.delete(p, false); System.out.println("删除成功"); }
            else System.out.println("文件不存在");
        }
    }

    static void createDeleteDir() throws Exception {
        System.out.print("HDFS目录路径: ");
        String hdfs = scanner.nextLine();
        Path p = new Path(hdfs);
        System.out.print("1-创建 2-删除: ");
        int c = scanner.nextInt();
        scanner.nextLine();
        if (c == 1) {
            fs.mkdirs(p);
            System.out.println("目录创建成功");
        } else {
            if (fs.exists(p)) {
                FileStatus[] files = fs.listStatus(p);
                if (files.length == 0) { fs.delete(p, false); System.out.println("空目录已删除"); }
                else System.out.println("目录非空，不删除");
            } else System.out.println("目录不存在");
        }
    }

    static void appendFile() throws Exception {
        System.out.print("HDFS文件路径: ");
        String hdfs = scanner.nextLine();
        System.out.print("追加内容: ");
        String content = scanner.nextLine();
        System.out.print("1-追加到开头 2-追加到结尾: ");
        int c = scanner.nextInt();
        scanner.nextLine();
        Path p = new Path(hdfs);
        if (c == 2) {
            FSDataOutputStream out = fs.append(p);
            out.writeUTF(content + "\n");
            out.close();
        } else {
            FSDataInputStream in = fs.open(p);
            byte[] old = new byte[(int) fs.getFileStatus(p).getLen()];
            in.readFully(old);
            in.close();
            fs.delete(p, false);
            FSDataOutputStream out = fs.create(p);
            out.writeUTF(content + "\n");
            out.write(old);
            out.close();
        }
        System.out.println("追加完成");
    }

    static void deleteFile() throws Exception {
        System.out.print("HDFS文件路径: ");
        String hdfs = scanner.nextLine();
        Path p = new Path(hdfs);
        if (fs.exists(p)) { fs.delete(p, false); System.out.println("删除成功"); }
        else System.out.println("文件不存在");
    }

    static void moveFile() throws Exception {
        System.out.print("源路径: ");
        String src = scanner.nextLine();
        System.out.print("目标路径: ");
        String dst = scanner.nextLine();
        fs.rename(new Path(src), new Path(dst));
        System.out.println("移动完成");
    }
}
