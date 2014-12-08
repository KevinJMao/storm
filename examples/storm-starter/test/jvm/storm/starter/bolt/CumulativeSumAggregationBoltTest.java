package storm.starter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.kevinmao.storm.AttackDetectionTopology;
import com.kevinmao.storm.CumulativeSumAggregationBolt;
import com.kevinmao.storm.GreyModelForecastingBolt;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;
import storm.starter.tools.MockTupleHelpers;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CumulativeSumAggregationBoltTest {
    private static final String ANY_NON_SYSTEM_COMPONENT_ID = "irrelevant_component_id";
    private static final String ANY_NON_SYSTEM_STREAM_ID = "irrelevant_stream_id";

    @Test()
    public void shouldOutputSomethingSane() {
        CumulativeSumAggregationBolt bolt = new CumulativeSumAggregationBolt();

        Map conf = mock(Map.class);
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector collector = mock(OutputCollector.class);

        bolt.prepare(conf, context, collector);

        Tuple input = MockTupleHelpers.mockTuple(ANY_NON_SYSTEM_COMPONENT_ID, ANY_NON_SYSTEM_STREAM_ID);
        when(input.getValueByField(AttackDetectionTopology.GREY_MODEL_ACTUAL_VOLUME_OUTPUT_FIELD)).thenReturn(32.8, 34.1, 35.8, 37.5, 40.1, 52.2, 75.5, 90.0, 150.0);
        when(input.getValueByField(AttackDetectionTopology.GREY_MODEL_FORECASTED_VOLUME_OUTPUT_FIELD)).thenReturn(37.02488, 38.40864, 40.55245, 42.65855, 45.78753, 60.07362, 95.687418, 130.38540, 199.81790);

        when(input.getValueByField(AttackDetectionTopology.LAST_TIMESTAMP_MEASURED)).thenAnswer(new Answer() {
            private Long count = 1L;

            public Object answer(InvocationOnMock invocation) {
                return count++;
            }
        });

        for(int i = 0; i < 9; i++) {
            bolt.execute(input);
        }
    }
}
