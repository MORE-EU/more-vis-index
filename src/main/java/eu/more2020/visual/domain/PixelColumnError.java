package eu.more2020.visual.domain;

public interface PixelColumnError {

    public int getId();
    public double getInnerColError(int measure);
    public double intraColError(int measure);
}
