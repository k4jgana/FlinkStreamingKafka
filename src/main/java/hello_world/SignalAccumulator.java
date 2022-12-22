package hello_world;

import hello_world.model.pojo.Signal;

public class SignalAccumulator {


    String key;

    Long start;
    Long end;
    long count=0;

    public SignalAccumulator()
    {}

    public SignalAccumulator(String key, long count) {
        this.key = key;
        this.count = count;
    }

    public SignalAccumulator add(Signal signal)
    {
        return new SignalAccumulator(
                signal.getKey(),
                this.count+1
        );
    }

    public String getCount()
    {
        return String.format("Number of occurences for key %s is %d",this.key,this.count);
    }


    public SignalAccumulator merge(SignalAccumulator other)
    {
        return new SignalAccumulator(
                this.key,this.count
        );
    }

}
