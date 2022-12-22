package hello_world;

import hello_world.model.pojo.Signal;
import org.apache.flink.api.common.eventtime.*;

import java.time.Duration;

public class SignalTimestampAndWatermarkStrategy implements WatermarkStrategy<Signal> {
    @Override
    public WatermarkGenerator<Signal> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<Signal>() {
            @Override
            public void onEvent(Signal signal, long l, WatermarkOutput watermarkOutput) {
                watermarkOutput.emitWatermark(new Watermark(signal.getTimestamp()));
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                watermarkOutput.emitWatermark(new Watermark(System.currentTimeMillis()));
            }
        };
    }

    @Override
    public TimestampAssigner<Signal> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return (element,recordTimestamp)->element.getTimestamp();
    }


}
