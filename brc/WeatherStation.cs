namespace brc;

public record WeatherStation(string Id, double MeanTemperature)
{
    public double Measurement()
    {
        double m = NextGaussian(MeanTemperature, 10);
        return Math.Round(m * 10.0) / 10.0;
    }
    
    private double NextGaussian(double mean,double variance )
    {
        var r = new Random();
        double u1 = r.NextDouble(); 
        double u2 = r.NextDouble();
        double randStdNormal = Math.Sqrt(-2.0 * Math.Log(u1)) *
                               Math.Sin(2.0 * Math.PI * u2);
        
        double randNormal =
            mean + variance * randStdNormal;

        return randNormal;
    }
}