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
using System.Linq;
using Amazon.Runtime.CredentialManagement;

namespace Gasmon
{
    class Program
    {

        public static async Task Main(string[] args)
        {
            ///External Credentials

            AWSCredentials credentials = GetGasmonCredentials();
            AmazonS3Client amazonS3Client = new AmazonS3Client(credentials, RegionEndpoint.EUWest1);
            Dictionary<string, JsonFile> locationsById = await GetJson(amazonS3Client);

            var sns = new AmazonSimpleNotificationServiceClient(credentials, RegionEndpoint.EUWest1);
            var sqs = new AmazonSQSClient(credentials, RegionEndpoint.EUWest1);
            var myTopicArn = "arn:aws:sns:eu-west-1:552908040772:EventProcessing-UCAS2-snsTopicSensorDataPart1-OVN4WSEGUZ58";

            var myQueueName = "Joana-GasMon-" + Guid.NewGuid();
            var myQueueUrl = (await sqs.CreateQueueAsync(myQueueName)).QueueUrl;

            await sns.SubscribeQueueAsync(myTopicArn, sqs, myQueueUrl);



            


            DateTime initTime = DateTime.Now;
            Console.WriteLine("The Initial time is: {0}", initTime);
            DateTime finalTime = initTime.AddMinutes(6);
           

            for (int i = 0; i < 6; i++)
            {
                Dictionary<string, MessageMsg> dictionary = new Dictionary<string, MessageMsg>();
                DateTime loopTime = initTime.AddMinutes(i + 1);
                do
                {
                    List<Amazon.SQS.Model.Message> messages = (await sqs.ReceiveMessageAsync(new ReceiveMessageRequest(myQueueUrl)
                    { WaitTimeSeconds = 1 })).Messages;
                    LoopingMessages(locationsById, dictionary, initTime, finalTime, messages);
                }
                while (DateTime.Now < loopTime);

                var fileName = "";
                fileName = WritingFile(dictionary, i, fileName, initTime);
   
            }


            Console.WriteLine("The Final time is: {0}", finalTime);


            Console.ReadKey();

        }

        private static async Task<Dictionary<string, JsonFile>> GetJson(AmazonS3Client amazonS3Client)
        {
            List<JsonFile> myJsonDeserialize = new List<JsonFile>();
            Dictionary<string, JsonFile> locationsById;

            using (var response = await amazonS3Client.GetObjectAsync("eventprocessing-ucas2-locationss3bucket-1dfub0iyuq3av", "locations.json"))
            {
                using (StreamReader reader = new StreamReader(response.ResponseStream))
                {
                    string contents = reader.ReadToEnd();

                    myJsonDeserialize = JsonConvert.DeserializeObject<List<JsonFile>>(contents);
                    locationsById = myJsonDeserialize.ToDictionary(location => location.id);

                    foreach (JsonFile json in myJsonDeserialize)
                    {
                        Console.WriteLine("x: {0}, y: {1}, id: {2}", json.x, json.y, json.id);
                    }
                }
            }

            return locationsById;
        }

        private static string WritingFile(Dictionary<string, MessageMsg> dictionary, int i, string fileName, DateTime initTime)
        {
            try
            {
                fileName = "file" + (i + 1);
                string path = @"C:\Work\Training\12.Gasmon\Gasmon\MessagesFiles\" + fileName + ".txt";
                using (StreamWriter file = File.CreateText(path))
                {
                    foreach (var group in dictionary.Values.GroupBy(l => l.locationId))
                    {
                        var locationId = group.Key;
                        double average = group.Average(l => l.value);
                        file.Write("LocationId: {0}\nValueAverage: {1} \n\n",
                            locationId, average);
                    }
                    //foreach (KeyValuePair<string, MessageMsg> keyValuePair in dictionary)
                    //{
                    //    file.Write("LocationId: {0}\nValue: {1}\nEventId: {2}\n\n",
                    //        keyValuePair.Key, keyValuePair.Value.locationId, keyValuePair.Value.value);
                    //}

                    file.Write("\nInitial time : {0}\n\n" +
                        "Total amount of messages recived was: {1}", initTime, dictionary.Count);
                }
            }
            catch (Exception Ex)
            {
                Console.WriteLine(Ex.ToString());
            }

            return fileName;
        }

        private static void LoopingMessages(Dictionary<string, JsonFile> locationsById, Dictionary<string, MessageMsg>
            dictionary, DateTime initialTime, DateTime finalTime, List<Message> messages)
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


                if (!dictionary.ContainsKey(snsMessageDeserialized.eventId) &&
                locationsById.ContainsKey(snsMessageDeserialized.locationId))
                {

                    if (timestampDate >= initialTime && timestampDate <= finalTime)
                    {
                        dictionary.TryAdd(snsMessageDeserialized.eventId, snsMessageDeserialized);
                        
                    }
                }
            }
            //////////////////////////////////////////////////
            

        }

        private static DateTime epoch2date(long epoch)
        {
            return new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddMilliseconds(epoch);
        }

        static AWSCredentials GetGasmonCredentials()
        {
            var chain = new CredentialProfileStoreChain();
            if (chain.TryGetAWSCredentials("gasmon", out var credentials)) return credentials;
            throw new InvalidOperationException("Missing AWS profile gasmon");
        }
    }
}

    


