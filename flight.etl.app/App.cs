using System.Diagnostics;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using flight.etl.app.Common;
using Microsoft.Extensions.Options;
using flight.etl.app.Pipelines;
using flight.etl.app.Services;

namespace flight.etl.app
{
    public class App
    {   
        static readonly Dictionary<EventType, JArray> EventsGroupedByType = new Dictionary<EventType, JArray>();
        FlightDataSettings _flightDataSettings;
        FlightEventValidationService _flightEventValidationService;

        public App(IOptions<FlightDataSettings> options, FlightEventValidationService flightEventValidationService)
        {
            _flightDataSettings = options.Value;
            _flightEventValidationService = flightEventValidationService;
        }

        public void StartBatchProcess()
        {
            foreach (string currentFile in Directory.EnumerateFiles(Path.Combine(_flightDataSettings.BaseDirectory, _flightDataSettings.InputDirectory)))
            {
                var fileProcessingTimeStamp = DateTime.Now.Ticks;

                Pipeline flightEtlPipeline = new Pipeline();

                flightEtlPipeline.Add(new ExtractFileProcess(_flightDataSettings, currentFile));
                //flightEtlPipeline.Add(new TransformEventsProcess(_flightEventValidationService));
                //flightEtlPipeline.Add(new LoadEventsProcess(_flightDataSettings, fileProcessingTimeStamp));

                flightEtlPipeline.Run();

                /*
                var fileName = Path.GetFileName(currentFile);

                Directory.Move(currentFile, Path.Combine(_flightDataSettings.BaseDirectory, _flightDataSettings.RawDirectory + "/" + fileName));

                string contents = File.ReadAllText(Path.Combine(_flightDataSettings.BaseDirectory, _flightDataSettings.RawDirectory + "/" + fileName));

                var eventsList = ParseEventJsonData(contents);

                foreach (JObject eventData in eventsList.Children<JObject>())
                {
                    Debug.WriteLine(eventData.ToString());
                    ValidateEvent(eventData);
                }
                */
            }
        }

        /*
        JArray ParseEventJsonData(string contents)
        {
            JArray eventsList = JArray.Parse(contents);
            return eventsList;
        }

        void ValidateEvent(JObject eventData) 
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

        void ValidateDepartureFile(JObject jsondata)
        {
            // validates the event and raises the exception
        }

        void ValidateArrivalFile(JObject jsondata)
        {
            // validates the event and raises the exception
        }

        void AddNewEventToEventGroup(EventType eventType, JObject eventData)
        {
            if (EventsGroupedByType.ContainsKey(eventType))
            {
                EventsGroupedByType[eventType].Add(eventData);
            }
        }

        void SaveGroupedEventsToFiles()
        {
            // iterate events and save them their files.
        }
        */
    }
}
