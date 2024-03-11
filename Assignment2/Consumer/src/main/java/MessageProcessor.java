import org.json.JSONObject;
import java.util.concurrent.ConcurrentHashMap;

// 专门处理消息的类
public class MessageProcessor {
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> skierRides;

  public MessageProcessor(ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> skierRides) {
    this.skierRides = skierRides;
  }

  public void processMessage(String message) throws Exception {
    // 解析消息
    JSONObject json = new JSONObject(message);
    String skierId = json.getString("skierId");
    String liftId = String.valueOf(json.getInt("liftId"));

    // 获取或创建滑雪者的乘坐详情Map
    ConcurrentHashMap<String, Integer> liftRides = skierRides.computeIfAbsent(skierId, k -> new ConcurrentHashMap<>());

    // 更新特定电梯的乘坐次数
    int newRideCount = liftRides.getOrDefault(liftId, 0) + 1;
    liftRides.put(liftId, newRideCount);


  }
}
