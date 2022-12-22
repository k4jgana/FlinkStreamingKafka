package hello_world;

import hello_world.model.pojo.Signal;
import org.apache.flink.api.common.functions.AggregateFunction;

public class SignalCountingAggregateFunction implements AggregateFunction<Signal,SignalAccumulator,String> {

    @Override
    public SignalAccumulator createAccumulator() {
        return new SignalAccumulator();
    }

    @Override
    public SignalAccumulator add(Signal signal, SignalAccumulator signalAccumulator) {
        return signalAccumulator.add(signal);
    }

    @Override
    public String getResult(SignalAccumulator signalAccumulator) {
        return signalAccumulator.getCount();
    }

    @Override
    public SignalAccumulator merge(SignalAccumulator signalAccumulator, SignalAccumulator acc1) {
        return signalAccumulator.merge(acc1);
    }
}
