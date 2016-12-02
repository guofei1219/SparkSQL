package utils;




import java.sql.*;

/**
 * Created by Michael on 2016/12/1.
 */
public class DataUtils {
    private static StringBuffer sb;

    private static Connection getConn() {
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://192.168.20.126:3306/hive";
        String username = "root";
        String password = "root";
        Connection conn = null;
        try {
            Class.forName(driver); //classLoader,加载对应驱动
            conn = (Connection) DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    private static String getHiveMetaData(String hiveTableName) {
        Connection conn = getConn();
        String sql = "SELECT\n" +
                "  #TBLS.`TBL_NAME`,\n" +
                "  #表名\n" +
                "  COLUMNS_V2.`COLUMN_NAME`\n" +
                "  #列名\n" +
                "  #COLUMNS_V2.`TYPE_NAME` #列类型\n" +
                "FROM\n" +
                "  TBLS #元数据信息表\n" +
                "  LEFT JOIN SDS #数据存储表\n" +
                "    ON TBLS.SD_ID = SDS.SD_ID\n" +
                "  LEFT JOIN CDS\n" +
                "    ON SDS.CD_ID = CDS.CD_ID\n" +
                "  LEFT JOIN COLUMNS_V2 #字段信息表\n" +
                "    ON CDS.CD_ID = COLUMNS_V2.CD_ID\n" +
                "WHERE TBLS.`TBL_NAME` = \"gd_py_corp_sharehd_info\"";
        //gd_py_corp_sharehd_info
        PreparedStatement pstmt;
        String result="";
        try {
            pstmt = (PreparedStatement)conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            int col = rs.getMetaData().getColumnCount();

            while (rs.next()) {
                for (int i = 1; i <= col; i++) {
                    result = result + rs.getString(i) + "\t";
                    //System.out.println(rs.getString(i));
                    //sb.append(rs.getString(i)).append("\t");
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void main(String args[]) {
        //DataUtils.getAll();
        String tmp = DataUtils.getHiveMetaData("gd_py_corp_sharehd_info");
        System.out.println(tmp);

    }
}
