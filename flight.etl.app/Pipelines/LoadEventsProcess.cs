using flight.etl.app.Common;
using flight.etl.app.Pipelines.Interface;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace flight.etl.app.Pipelines
{
    public class LoadEventsToFileProcess : IPipelineProcess
    {
        Dictionary<string, JArray> _curatedEventsGroupedByType;
        Dictionary<string, JArray> _exceptionEventsGroupedByType;

        public bool IsComplete { get; private set; }

        public FlightDataSettings FlightDataSettings { get; set; }
        public long ProcessingTimeStamp { get; set; }
        public List<string> PipelineSummary { get; set; }

        public LoadEventsToFileProcess(List<string> pipelineSummary, FlightDataSettings flightDataSettings, long processingTimeStamp)
        {
            ProcessingTimeStamp = processingTimeStamp != 0 ? processingTimeStamp : throw new Exception("Processing timestamp has not been set");
            FlightDataSettings = flightDataSettings ?? throw new Exception("Flight Data directory Settings has not been set");
            PipelineSummary = pipelineSummary;
        }

        public void Connect(IPipelineProcess next)
        {
            throw new InvalidOperationException("No output from this process");
        }

        public void Process()
        {
            try
            {
                SaveGroupedEventsToFiles();
                IsComplete = true;
                Debug.WriteLine("Events has been succesfully saved to respective curated and exception folders.");
            }
            catch (Exception e)
            {
                Debug.WriteLine(e.Message);
                Debug.WriteLine("Failed to load events to respective curated and exception folder.");
            }
        }

        public void SetInput<T>(T transformedEvents)
        {
            var _transformedEvents = (Tuple<Dictionary<string, JArray>, Dictionary<string, JArray>>)(object)transformedEvents;
            _curatedEventsGroupedByType = _transformedEvents.Item1;
            _exceptionEventsGroupedByType = _transformedEvents.Item2;
        }

        void SaveGroupedEventsToFiles()
        {
            // writing curated event file
            foreach (var eventType in _curatedEventsGroupedByType.Keys)
            {
                var eventFileName = eventType + "-" + ProcessingTimeStamp + ".json";

                try
                {
                    var eventFilePath = Path.Combine(FlightDataSettings.BaseDirectory, FlightDataSettings.CuratedDirectory + "/" + eventType + "/" + eventFileName);
                    File.WriteAllText(eventFilePath, _curatedEventsGroupedByType[eventType].ToString());                    
                }
                catch (Exception e)
                {
                    Debug.WriteLine("Unable to write file " + eventFileName);
                    Debug.WriteLine(e.Message);
                }                
            }

            // writing exception event file
            foreach (var eventType in _curatedEventsGroupedByType.Keys)
            {
                var eventFileName = eventType + "-" + ProcessingTimeStamp + ".json";
                try
                {                   
                    var eventFilePath = Path.Combine(FlightDataSettings.BaseDirectory, FlightDataSettings.ExceptionDirectory + "/" + eventType + "/" + eventFileName);
                    File.WriteAllText(eventFilePath, _exceptionEventsGroupedByType[eventType].ToString());
                }
                catch (Exception e)
                {
                    Debug.WriteLine("Unable to write file " + eventFileName);
                    Debug.WriteLine(e.Message);
                }
            }
        }
    }
}
