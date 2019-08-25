package com.h3c.hdfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.io.IOUtils;

public class SimpleExample {
    private final static String PATH_TO_HDFS_SITE_XML = "/etc/hadoop/conf/hdfs-site.xml";
    private final static String PATH_TO_CORE_SITE_XML = "/etc/hadoop/conf/core-site.xml";

    private final static String JVM_KRB5_CONF_PARM = "java.security.krb5.conf";
    private final static String JVM_KRB5_CONF_PARM_VALUE = "/etc/krb5.conf";
    private final static String USER_PRINCIPAL = "diao2@HDE.H3C.COM";
    private final static String PATH_TO_USER_KEYTAB = "/etc/security/keytabs/diao2.keytab";

    public static void main(String[] args) throws Exception {
        //创建conf实例对象，并添加hdfs相关配置文件
        Configuration conf = new Configuration();
        conf.addResource(new Path(PATH_TO_HDFS_SITE_XML));
        conf.addResource(new Path(PATH_TO_CORE_SITE_XML));

        // kerberos身份认证
        System.setProperty(JVM_KRB5_CONF_PARM, JVM_KRB5_CONF_PARM_VALUE);
        UserGroupInformation.setConfiguration(conf);
        // 安全登录，指定登录用户的票据与keytab
        UserGroupInformation.loginUserFromKeytab(USER_PRINCIPAL, PATH_TO_USER_KEYTAB);
        System.out.println("kerberos auth success");
        //拿到一个文件系统操作的客户端实例对象
        FileSystem fs = FileSystem.get(conf);

        //创建目录
        final String CREATE_DIR_DEST_PATH = "/hdfstest";
        Path create_dir_destPath = new Path(CREATE_DIR_DEST_PATH);
        if (!fs.exists(create_dir_destPath)) {
            fs.mkdirs(create_dir_destPath);
            System.out.println("success to create path " + CREATE_DIR_DEST_PATH);
        } else {
            System.out.println(CREATE_DIR_DEST_PATH + "already exist");
        }


        /*// 新建文件，并写入数据
        final String FILE_CONTENT = "It is successful to create new file if you can see me";
        final String CREATE_FILE_DEST_PATH="/hdfstest";
        final String CREATE_FILE_NAME="writetest";
        FSDataOutputStream create_file_out = null;
        try {
            create_file_out = fs.create(new Path(CREATE_FILE_DEST_PATH + File.separator + CREATE_FILE_NAME));
            create_file_out.write(FILE_CONTENT.getBytes());
            create_file_out.hsync();
            System.out.println("success to write.");
        } finally {
            // make sure the stream is closed finally.
            IOUtils.closeStream(create_file_out);
        }

        // 向某个文件追加内容
        final String APPEND_CONTENT = "I append this content.\n";
        final String APPEND_DEST_PATH="/hdfstest";
        final String APPEND_FILE_NAME="appendtest";
        FSDataOutputStream append_out = null;
        try {
            append_out = fs.append(new Path(APPEND_DEST_PATH + File.separator + APPEND_FILE_NAME));
            append_out.write(APPEND_CONTENT.getBytes());
            append_out.hsync();
            System.out.println("success to append.");
        } finally {
            // make sure the stream is closed finally.
            IOUtils.closeStream(append_out);
        }

        //读文件
        final String READ_DEST_PATH="/hdfstest";
        final String READ_FILE_NAME="readtest";
        final String READ_STR_PATH = READ_DEST_PATH + File.separator + READ_FILE_NAME;
        Path path = new Path(READ_STR_PATH);
        FSDataInputStream in = null;
        BufferedReader reader = null;
        StringBuffer strBuffer = new StringBuffer();
        try {
            in = fs.open(path);
            reader = new BufferedReader(new InputStreamReader(in));
            String sTempOneLine;
            // write file
            while ((sTempOneLine = reader.readLine()) != null) {
                strBuffer.append(sTempOneLine);
            }
            System.out.println("result is : " + strBuffer.toString());
            System.out.println("success to read.");
        } finally {
            // make sure the streams are closed finally.
            IOUtils.closeStream(reader);
            IOUtils.closeStream(in);
        }

        //删除文件
        final String DEL_DEST_PATH="/hdfstest";
        final String DEL_FILE_NAME="deltest";
        Path beDeletedPath = new Path(DEL_DEST_PATH + File.separator + DEL_FILE_NAME);
        if (fs.delete(beDeletedPath, true)) {
            System.out.println("success to delete the file " + DEL_DEST_PATH + File.separator + DEL_FILE_NAME);
        } else {
            System.out.println("failed to delete the file " + DEL_DEST_PATH + File.separator + DEL_FILE_NAME);
        }

        //递归删除目录
        final String DEL_RECU_DEST_PATH="/hdfstest";
        Path del_recu_destPath = new Path(DEL_RECU_DEST_PATH);
        if (!fs.exists(del_recu_destPath)) {
            System.out.println(DEL_RECU_DEST_PATH + "doesn't exist!");
            return;
        }
        if (!fs.delete(del_recu_destPath, true)) {
            System.out.println("failed to delete destPath " + DEL_RECU_DEST_PATH);
        }
        System.out.println("success to delete path " + DEL_RECU_DEST_PATH);

        //设置存储策略
        final String SET_POLICY_DEST_PATH="/aaa";
        String policyName="WARM";
        if (fs instanceof DistributedFileSystem) {
            DistributedFileSystem dfs = (DistributedFileSystem) fs;
            Path set_policy_destPath = new Path(SET_POLICY_DEST_PATH);
            Boolean flag = false;
            BlockStoragePolicy[] storage = dfs.getStoragePolicies();

            for (BlockStoragePolicy bs : storage) {
                if (bs.getName().equals(policyName)) {
                    flag = true;
                }
                System.out.println("StoragePolicy:" + bs.getName());
            }
            if (!flag) {
                policyName = storage[0].getName();
            }
            dfs.setStoragePolicy(set_policy_destPath, policyName);
            System.out.println("success to set Storage Policy path " + SET_POLICY_DEST_PATH);
        } else {
            System.out.println("SmallFile not support to set Storage Policy !!!");
        }


        //需要在公共代码中填写的代码
        //多线程
        final int THREAD_COUNT = 2;
        for (int threadNum = 0; threadNum < THREAD_COUNT; threadNum++) {
            HdfsExampleThread example_thread = new HdfsExampleThread("hdfs_example_" + threadNum);
            example_thread.start();
        }*/

    }
}

//多线程实例中同文件下增加的两个类
class HdfsExampleThread extends Thread {

    public HdfsExampleThread(String threadName) {
        super(threadName);
    }

    public void run() {
        HdfsExample example;
        try {
            example = new HdfsExample("/user/hdfstest/" + getName() + "write.txt");
            example.write();
        } catch (IOException e) {
            System.out.println(e);
        }
    }
}

class HdfsExample {
    private final static String PATH_TO_HDFS_SITE_XML = "/etc/hadoop/conf/hdfs-site.xml";
    private final static String PATH_TO_CORE_SITE_XML = "/etc/hadoop/conf/core-site.xml";
    private final String content = "It is successful to create new file if you can see me";
    private final static String JVM_KRB5_CONF_PARM = "java.security.krb5.conf";
    private final static String JVM_KRB5_CONF_PARM_VALUE = "/etc/krb5.conf";
    private final static String USER_PRINCIPAL = "diao2@HDE.H3C.COM";
    private final static String PATH_TO_USER_KEYTAB = "/etc/security/keytabs/diao2.keytab";
    String path = "";

    public HdfsExample(String path) {
        this.path = path;
    }

    public void write() throws IOException {
        Configuration conf = new Configuration();
        conf.addResource(new Path(PATH_TO_HDFS_SITE_XML));
        conf.addResource(new Path(PATH_TO_CORE_SITE_XML));

         // kerberos身份认证
         System.setProperty(JVM_KRB5_CONF_PARM, JVM_KRB5_CONF_PARM_VALUE);
         UserGroupInformation.setConfiguration(conf);
         // 安全登录，指定登录用户的票据与keytab
         UserGroupInformation.loginUserFromKeytab(USER_PRINCIPAL,PATH_TO_USER_KEYTAB);
         System.out.println("kerberos auth success");
        //拿到一个文件系统操作的客户端实例对象
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream out = null;
        try {
            out = fs.create(new Path(path));
            out.write(content.getBytes());
            out.hsync();
            System.out.println("success to write.");
        } finally {
            // make sure the stream is closed finally.
            IOUtils.closeStream(out);
        }
    }
}
