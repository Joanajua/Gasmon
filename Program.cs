using System;
using Amazon.S3;
using Amazon.SQS;
using Amazon.SimpleNotificationService;
using Amazon.Runtime;
using Amazon;
using System.Threading.Tasks;
using System.IO;
using System.Collections.Generic;
using Amazon.SQS.Model;
using Newtonsoft.Json;
using System.Timers;
using System.Linq;


namespace Gasmon
{
    class Program
    {

        public static async Task Main(string[] args)
        {
            
            BasicAWSCredentials credentials = new BasicAWSCredentials("", "");
            AmazonS3Client amazonS3Client = new AmazonS3Client(credentials, RegionEndpoint.EUWest1);
            List<JsonFile> myJsonDeserialize = new List<JsonFile>();
            Dictionary<string, JsonFile> locationsById;

            using (var response = await amazonS3Client.GetObjectAsync("eventprocessing-ucas2-locationss3bucket-1dfub0iyuq3av", "locations.json"))
            {
                using (StreamReader reader = new StreamReader(response.ResponseStream))
                {
                    string contents = reader.ReadToEnd();

                    //A JsonFile created. Making a list to put the jsonFile information into it.
                    myJsonDeserialize = JsonConvert.DeserializeObject<List<JsonFile>>(contents);
                    locationsById = myJsonDeserialize.ToDictionary(location => location.id);

                    foreach (JsonFile json in myJsonDeserialize)
                    { 
                        Console.WriteLine("x: {0}, y: {1}, id: {2}", json.x, json.y, json.id);
                    }
                }
            }

            //Part of the AWSSKD.SimpleNotificationService NuGet package.
            var sns = new AmazonSimpleNotificationServiceClient(credentials, RegionEndpoint.EUWest1);
            //Part of the AWSSDK.SQS NuGet package.
            var sqs = new AmazonSQSClient(credentials, RegionEndpoint.EUWest1);
            //The topic ARN supplied
            var myTopicArn = "arn:aws:sns:eu-west-1:552908040772:EventProcessing-UCAS2-snsTopicSensorDataPart1-OVN4WSEGUZ58";
            //// Create a new queue. You can't reuse a queue name within 60 seconds so use a GUID
            // to generate a new random name.
            var myQueueName = "Joana-GasMon-" + Guid.NewGuid();
            var myQueueUrl = (await sqs.CreateQueueAsync(myQueueName)).QueueUrl;
            //In order for a queue to receive messages from a topic,
            //it needs to be subscribed and also needs a custom security
            //policy to allow the topic to deliver messages to the queue
            await sns.SubscribeQueueAsync(myTopicArn, sqs, myQueueUrl);



            Dictionary<string, MessageMsg> EventsId = new Dictionary<string, MessageMsg>();
            


            DateTime initialTime = DateTime.Now;
            DateTime finalTime = initialTime.AddMinutes(30);
            TimeSpan totalTime = finalTime - initialTime;
            Console.WriteLine("The Initial time is: {0}", initialTime);
            

            do
            {
                List<Amazon.SQS.Model.Message> messages = (await sqs.ReceiveMessageAsync(new ReceiveMessageRequest(myQueueUrl) { WaitTimeSeconds = 1 })).Messages;
                CollectMessages(locationsById, EventsId, initialTime, finalTime, messages);

            }
            while (DateTime.Now < finalTime);
            
            

            try
            {
                string path = @"C:\Work\Training\12.Gasmon\Gasmon\MessagesFiles\file.txt";
                using (StreamWriter file = File.CreateText(path))
                {
                    foreach(KeyValuePair<string, MessageMsg> keyValuePair in EventsId)
                    {
                        file.Write("EventId: {0}\nLocationId: {1}\nValue: {2}\nTimestamp: {3}\n\n",
                            keyValuePair.Key, keyValuePair.Value.locationId, keyValuePair.Value.value, 
                            keyValuePair.Value.timestamp);
                    }

                    file.Write("\nTotal time passed is: {0}\n\n" +
                        "Total amount of messages recived was: {1}", totalTime, EventsId.Count);
                }
            }
            catch (Exception Ex)
            {
                Console.WriteLine(Ex.ToString());
            }

            
            Console.WriteLine("The Final time is: {0}", finalTime);
            

            Console.ReadKey();

        }

        private static void CollectMessages(Dictionary<string, JsonFile> locationsById, Dictionary<string, MessageMsg> EventsId, DateTime initialTime, DateTime finalTime, List<Amazon.SQS.Model.Message> messages)
        {
            foreach (var message in messages)
            {

                var snsMessage = Amazon.SimpleNotificationService.Util.Message.ParseMessage(message.Body);

                MessageMsg snsMessageDeserialized = JsonConvert.DeserializeObject<MessageMsg>(snsMessage.MessageText);

                DateTime timestampDate = epoch2date(snsMessageDeserialized.timestamp);

                Console.WriteLine("The MESSAGE is: \n locationId: " + snsMessageDeserialized.locationId +
                    "\n eventId: " + snsMessageDeserialized.eventId + "\n value: "
                    + snsMessageDeserialized.value + "\n timestamp: " + snsMessageDeserialized.timestamp
                     + "\n timestamp: " + timestampDate);


                if (!EventsId.ContainsKey(snsMessageDeserialized.eventId) &&
                    locationsById.ContainsKey(snsMessageDeserialized.locationId))
                {
                    //Checking messages are not doubled
                    //making sure just taking messages from our sensors
                    if (timestampDate >= initialTime && timestampDate <= finalTime)
                        //taking all messages sended during our timespam (1min) even if they arrive late
                    {
                        EventsId.TryAdd(snsMessageDeserialized.eventId, snsMessageDeserialized);
                    }
                }
            }
        }

        private static DateTime epoch2date(long epoch)
        {
            return new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddMilliseconds(epoch);
        }
    }
}

    


