#r "Microsoft.Azure.Devices.Client"
#r "Newtonsoft.Json"

using System.IO;
using Microsoft.Azure.Devices.Client;
using Newtonsoft.Json;

static int redThreshold { get; set; } = 300;
static int amberThreshold { get; set; } = 260;
static int greenThreshold { get; set;} = 250;

public static async Task Run(Message messageReceived, IAsyncCollector<Message> output, TraceWriter log)
{
    byte[] messageBytes = messageReceived.GetBytes();
    var messageString = System.Text.Encoding.UTF8.GetString(messageBytes);

    log.Info("Message received");
    log.Info(messageString);

    
    if (!string.IsNullOrEmpty(messageString))
    {
        // Get the body of the message and deserialize it
        log.Info("deserialize");
        var data = JsonConvert.DeserializeObject<Data[]>(messageString);
        log.Info("Done. Count = " + data.Length);

        if (data.Length >= 1)
        {
            log.Info("count >= 1");
            
            for(int i = 0; i < data.Length; i++)
            {
                var hwID = data[i].HwId;
                if (data[i].DisplayName.Equals("Temperature"))
                {
                    var currentTemp = Int32.Parse(data[i].Value);
                   
                    if (currentTemp > redThreshold) {
                        log.Info("Temp RED alarm: " + currentTemp);
                        using (var stream = GenerateStreamFromString("{\"HwId\":\"" + hwID + "\",\"UId\":\"1\",\"Address\":\"40011\",\"Value\":\"2\"}"))
                        {
                            var deviceMessage = new Message(stream);
                            deviceMessage.Properties.Add("command-type","ModbusWrite");
                            await output.AddAsync(deviceMessage);
                        }  
                    } else if (currentTemp > amberThreshold) {
                        log.Info("Temp AMBER alarm: " + currentTemp);
                        using (var stream = GenerateStreamFromString("{\"HwId\":\"" + hwID + "\",\"UId\":\"1\",\"Address\":\"40011\",\"Value\":\"1\"}"))
                        {
                            var deviceMessage = new Message(stream);
                            deviceMessage.Properties.Add("command-type","ModbusWrite");
                            await output.AddAsync(deviceMessage);
                        }                          
                    } else if (currentTemp < greenThreshold) {
                        log.Info("Temp GREEN no alarm: " + currentTemp);
                        using (var stream = GenerateStreamFromString("{\"HwId\":\"" + hwID + "\",\"UId\":\"1\",\"Address\":\"40011\",\"Value\":\"0\"}"))
                        {
                            var deviceMessage = new Message(stream);
                            deviceMessage.Properties.Add("command-type","ModbusWrite");
                            await output.AddAsync(deviceMessage);
                        }                          
                    }
                }  
         
            }

        } else {
            // do nothing;
        }
        
    }

}


public static Stream GenerateStreamFromString(string s)
{
    var stream = new MemoryStream();
    var writer = new StreamWriter(stream);
    writer.Write(s);
    writer.Flush();
    stream.Position = 0;
    return stream;
}



public class Data
{
    public string DisplayName { get; set; }
    public string HwId { get; set; }
    public string Address { get; set; }
    public string Value { get; set; }
    public string SourceTimestamp { get; set; }
}

public class DeviceMessage
{
    public string HwId { get; set;}
    public string UId { get; set;}
    public string Address {get ; set;}
    public string Value {get ; set;}
}