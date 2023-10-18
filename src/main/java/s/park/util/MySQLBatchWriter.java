package s.park.util;

import com.mysql.cj.jdbc.Driver;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

@Slf4j
public class MySQLBatchWriter extends ForeachWriter<Row> implements Serializable {

  private static final long serialVersionUID = 46847764416872707L;

  private Connection connection;
  private PreparedStatement statement;
  private String detectTime;
  // "attack_count=attack_count+VALUES(attack_count),detect_time=VALUES(detect_time)";

  // 初始化连接和预编译的插入语句
  @Override
  public boolean open(long partitionId, long version) {
    try {
      // 数据库连接参数
      String user = "root";
      String password = "rootpass";
      String url = "jdbc:mysql://172.22.105.146:3306/flow_analyze";
      Properties info = new Properties();
      info.setProperty("user", user);
      info.setProperty("password", password);
      connection = new Driver().connect(url, info);
      String sql = "INSERT INTO statistic" +
          "(ip_first, ip_second, attack_type, attack_count, detect_time)" +
          "VALUES (?, ?, ?, ?, ?) " + // AS alias
          "ON DUPLICATE KEY UPDATE " +
          "attack_count=?,detect_time=?";
      // attack_count=alias.attack_count+statistic.attack_count;
      statement = connection.prepareStatement(sql);
      detectTime = String.valueOf(System.currentTimeMillis());
      // log.warn("\033[32;1m================= 建立连接  =================\033[0m");
      return true;

    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }
  }


  @Override
  /// 构建 PreparedStatement (sql语句)
  public void process(Row value) {
    try {
      statement.setString(1, value.getString(0)); // ip 1
      statement.setString(2, value.getString(1)); // ip 2
      statement.setString(3, value.getString(2)); // attack_type
      statement.setLong(4, value.getLong(3));     // attack_count
      statement.setString(5, detectTime);         // detectTime
      // On duplicate: 
      statement.setLong(6, value.getLong(3));
      statement.setString(7, detectTime);
      statement.executeUpdate();
    } catch (SQLException e) {
      log.warn("{}", statement.toString());
      e.printStackTrace();
    }
  }

  // 关闭连接和资源
  @Override
  public void close(Throwable errorOrNull) {
    // log.warn("================= 释放连接 =================");
    try {
      if (statement != null) {
        statement.close();
      }
      if (connection != null) {
        connection.close();
      }
    } catch (SQLException e) {
      errorOrNull.printStackTrace();
      e.printStackTrace();
    }
  }
}
