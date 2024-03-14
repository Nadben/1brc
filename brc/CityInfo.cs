namespace brc;

public class CityInfo
{
    public CityInfo(double temperature)
    {
        Max = temperature;
        Min = temperature;
        SumTemperature = temperature;
    }
    public double Max { get; set; }
    public double Min { get; set; }
    public double SumTemperature { get; set; }
    public double Average => Math.Round(SumTemperature / Count * 10.0) / 10.0;
    private int Count { get; set; } = 1;
    
    public void IncrementCount()
    {
        Count++;
    }
}