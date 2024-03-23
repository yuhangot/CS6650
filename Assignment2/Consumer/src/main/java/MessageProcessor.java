import org.json.JSONObject;
import java.util.concurrent.ConcurrentHashMap;

public class MessageProcessor {
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> skierRides;

  public MessageProcessor(ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> skierRides) {
    this.skierRides = skierRides;
  }

  public void processMessage(String message) throws Exception {
    JSONObject json = new JSONObject(message);
    String skierId = json.getString("skierId");
    String liftId = String.valueOf(json.getInt("liftId"));

    ConcurrentHashMap<String, Integer> liftRides = skierRides.computeIfAbsent(skierId, k -> new ConcurrentHashMap<>());
    
    int newRideCount = liftRides.getOrDefault(liftId, 0) + 1;
    liftRides.put(liftId, newRideCount);


  }
}
