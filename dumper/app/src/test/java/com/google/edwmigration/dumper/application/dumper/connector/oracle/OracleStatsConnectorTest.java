import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class OracleStatsConnectorTest {

  @Test
  public void getConnectorScope_success() {
    OracleStatsConnector connector = new OracleStatsConnector();
    assertEquals(OracleConnectorScope.STATS, connector.getConnectorScope());
  }
}
