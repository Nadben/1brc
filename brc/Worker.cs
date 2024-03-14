using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;

namespace brc;

public class Worker
{

    private ConcurrentDictionary<string, CityInfo> _cityInfos = new();
    
    private readonly Stopwatch _processingStopwatch = new();
    private readonly Stopwatch _displayingStopwatch = new();
    private readonly List<byte> _rejectedArrayBytes = new();
    private readonly BlockingCollection<byte[]> _processedBuffer = new();
    
    private const string _file = @"D:\\repos\\brc\\brc\\measurement.txt";

    public void DoWork()
    {
        Console.WriteLine("Processing starting");
        _processingStopwatch.Start();
        var consumers = Task.Run(ConsumeDataFromStream);
        var producer = Task.Run(() => ReadTextAsync(_file));
        Task.WaitAll(producer, consumers);
        _processingStopwatch.Stop();
        Console.WriteLine("processing ended in {0} s", _processingStopwatch.Elapsed.Seconds);
        
        Console.WriteLine("Displaying results on screen");
        _displayingStopwatch.Start();
        foreach (var (key, value) in _cityInfos)
        {
            Console.WriteLine("{0} City has an average temperature of {1} with a low of {2} and a high of {3}", 
                key,
                value.Average,
                value.Min, 
                value.Max);
        }
        _displayingStopwatch.Stop();
        Console.WriteLine("Displaying ended in {0} s", _displayingStopwatch.Elapsed.Seconds);

    }
    
    private async Task ReadTextAsync(string filePath)
    {
        using var sourceStream =
            new FileStream(
                filePath,
                FileMode.Open, FileAccess.Read, FileShare.Read,
                bufferSize: 65536, useAsync: true);

        int numRead;
        byte[] buffer = new byte[0x10000];
        
        while ((numRead = await sourceStream.ReadAsync(buffer, 0, buffer.Length)) != 0)
        {
            await Task.Run(() => ParseBuffer(buffer, 65536));
        }
        _processedBuffer.CompleteAdding();
    }

    private void ParseBuffer(byte[] buffer, int bufferSize)
    {
        var cleanedBuffer = new byte[bufferSize + _rejectedArrayBytes.Count];
        int startingSize = _rejectedArrayBytes.Count;

        if (_rejectedArrayBytes.Count != 0)
        {
            Array.Copy(Enumerable.Reverse(_rejectedArrayBytes).ToArray(), 
                0,
                cleanedBuffer,
                0,
                _rejectedArrayBytes.Count);
            
            _rejectedArrayBytes.Clear();
        }
        
        for (var i = buffer.Length - 1; i >= 0; i--)
        {
            var byteToCheck = buffer[i];
            
            if (byteToCheck != 10)
            {
                _rejectedArrayBytes.Add(byteToCheck);
            }

            if (byteToCheck != 10) continue;
            
            Array.Copy(buffer,
                0 ,
                cleanedBuffer,
                startingSize,
                i + 1);
                
            if (cleanedBuffer[^1] == 0)
            {
                for (int j = cleanedBuffer.Length - 1; j >= 0; j--)
                {
                    if (cleanedBuffer[j] != 0)
                    {
                        Array.Resize(ref cleanedBuffer, j + 1);
                        break;
                    }
                }
            }
                
            _processedBuffer.Add(cleanedBuffer);
            break;
        }
    }
    
    private async void ConsumeDataFromStream()
    {
        while (!_processedBuffer.IsCompleted)
        {
            foreach (var bytes in _processedBuffer.GetConsumingEnumerable())
            {
                await Task.Run(() => GetLines(bytes));
            }
        }
        _processorForLineOfBytes.CompleteAdding();
    }

    private record ByteLinesConverted(byte[] Bytes)
    {
        public string[] LineSplit => Encoding.UTF8.GetString(Bytes).Split(";");
    }

    private BlockingCollection<ByteLinesConverted> _processorForLineOfBytes = new();
    private async void GetLines(byte[] bytes)
    {
        await Task.Run(() =>
        {
            var index = 0;
            for (var i = 0; i < bytes.Length; i++)
            {
                if (bytes[i] == 0xA)
                {
                    var byteLines = new byte[i - index];
                    
                    Array.Copy(bytes, index, byteLines, 0, i - index);
                    index = i + 1;
                    var byteLinesConverted = new ByteLinesConverted(Bytes: byteLines);
                    try
                    {
                        var id = byteLinesConverted.LineSplit[0];
                        var temperature = double.Parse(byteLinesConverted.LineSplit[1]);
                        _cityInfos.AddOrUpdate(id, new CityInfo(temperature), (s, cityInfo) =>
                        {
                            if (temperature < cityInfo.Min)
                            {
                                cityInfo.Min = temperature;
                            }
                            
                            if (temperature > cityInfo.Max)
                            {
                                cityInfo.Max = temperature;
                            }

                            cityInfo.SumTemperature += temperature;
                            cityInfo.IncrementCount();
                            
                            return cityInfo;
                        });
                    }
                    catch (Exception e)
                    {
                        // ignored
                    }
                }
            }
        });
    }
}