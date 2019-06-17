using System.Diagnostics;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections;
using System.Collections.Generic;

namespace flight.etl.app
{
    public enum EventType
    {
        Departure,
        Arrival
    }

    class Program
    {
        static readonly string BaseDirectory = "C:/Projects/flight.etl.app/ProcessorRoot/";
        static readonly string InputDirectory = "01 - Input";
        static readonly string RawDirectory = "02 - RAW";
        static readonly string ExceptionDirectory = "03 - Exception";
        static readonly string CuratedDirectory = "04 - Curated";
        static long FileProcessingTimeStamp;
        static readonly Dictionary<EventType, JArray> EventsGroupedByType = new Dictionary<EventType, JArray>();

        static void Main(string[] args)
        {
            foreach (string currentFile in Directory.EnumerateFiles(Path.Combine(BaseDirectory, InputDirectory)))
            {
                FileProcessingTimeStamp = DateTime.Now.Ticks;

                var fileName = Path.GetFileName(currentFile);

                Directory.Move(currentFile, Path.Combine(BaseDirectory, RawDirectory));

                string contents = File.ReadAllText(Path.Combine(BaseDirectory, RawDirectory + "/" + fileName));

                var eventsList = ParseEventJsonData(contents);

                foreach (JObject eventData in eventsList.Children<JObject>())
                {
                    Debug.WriteLine(eventData.ToString());
                    ValidateEvent(eventData);
                }
            }
        }

        static JArray ParseEventJsonData(string contents)
        {
            JArray eventsList = JArray.Parse(contents);
            return eventsList;
        }

        static void ValidateEvent(JObject eventData) 
        {
            var eventType = eventData["eventType"].ToString();
            if (eventType == "Departure")
            {
                Debug.WriteLine("Departure data detected");
                try
                {
                    ValidateDepartureFile(eventData);
                }
                catch(Exception e)
                {
                    // log error
                }
            }
            else if (eventType == "Arrival")
            {
                Debug.WriteLine("Arrival data detected");
                try
                {
                    ValidateArrivalFile(eventData);
                }
                catch (Exception e)
                {
                    // log error
                }
            }
        }

        static void ValidateDepartureFile(JObject jsondata)
        {
            // validates the event and raises the exception
        }

        static void ValidateArrivalFile(JObject jsondata)
        {
            // validates the event and raises the exception
        }

        static void AddNewEventToEventGroup(EventType eventType, JObject eventData)
        {
            if (EventsGroupedByType.ContainsKey(eventType))
            {
                EventsGroupedByType[eventType].Add(eventData);
            }
        }

        static void SaveGroupedEventsToFiles()
        {
            // iterate events and save them their files.
        }

    }

}
