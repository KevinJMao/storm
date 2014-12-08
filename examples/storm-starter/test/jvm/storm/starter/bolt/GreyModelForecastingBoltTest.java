package storm.starter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.kevinmao.storm.AttackDetectionTopology;
import com.kevinmao.storm.GreyModelForecastingBolt;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;
import storm.starter.tools.MockTupleHelpers;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GreyModelForecastingBoltTest {
    private static final String ANY_NON_SYSTEM_COMPONENT_ID = "irrelevant_component_id";
    private static final String ANY_NON_SYSTEM_STREAM_ID = "irrelevant_stream_id";
    private static final Object ANY_OBJECT = new Object();

    @Test()
    public void shouldOutputSomethingSane() {
        GreyModelForecastingBolt bolt = new GreyModelForecastingBolt(5);

        Map conf = mock(Map.class);
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector collector = mock(OutputCollector.class);

        bolt.prepare(conf, context, collector);

        Tuple input = MockTupleHelpers.mockTuple(ANY_NON_SYSTEM_COMPONENT_ID, ANY_NON_SYSTEM_STREAM_ID);
        when(input.getValueByField(AttackDetectionTopology.COUNTER_BOLT_PACKET_COUNT_FIELD)).thenReturn(26.7, 31.5, 32.8, 34.1, 35.8, 37.5);
//        when(input.getValueByField(AttackDetectionTopology.COUNTER_BOLT_PACKET_COUNT_FIELD)).thenAnswer(new Answer() {
//            private Random rng = new Random(System.currentTimeMillis());
//            private Long count = 0L;
//            public Object answer(InvocationOnMock invocation) {
////                return Math.abs(rng.nextLong()) % 100L;
//                return count++;
//            }
//        });
        when(input.getValueByField(AttackDetectionTopology.LAST_TIMESTAMP_MEASURED)).thenAnswer(new Answer() {
            private Long count = 1L;

            public Object answer(InvocationOnMock invocation) {
                return count++;
            }
        });

        for(int i = 0; i < 6; i++) {
            bolt.execute(input);
        }
    }
}
