using flight.etl.app.Common;
using flight.etl.app.Pipelines.Interface;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace flight.etl.app.Pipelines
{
    public class LoadEventsProcess : IPipelineProcess
    {
        Dictionary<EventType, JArray> _curatedEventsGroupedByType;
        Dictionary<EventType, JArray> _exceptionEventsGroupedByType;

        public bool IsComplete { get; private set; }

        public FlightDataSettings FlightDataSettings { get; set; }
        public long ProcessingTimeStamp { get; set; }

        public LoadEventsProcess(FlightDataSettings flightDataSetting, long processingTimeStamp)
        {
            FlightDataSettings = flightDataSetting;
            ProcessingTimeStamp = processingTimeStamp;
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
            }
            catch (Exception e)
            {
                Debug.WriteLine(e.Message);
            }
        }

        public void SetInput<T>(T transformedEvents)
        {
            var _transformedEvents = (Tuple<Dictionary<EventType, JArray>, Dictionary<EventType, JArray>>)(object)transformedEvents;
            _curatedEventsGroupedByType = _transformedEvents.Item1;
            _exceptionEventsGroupedByType = _transformedEvents.Item2;
        }

        void SaveGroupedEventsToFiles()
        {
            // iterate events and save them their files.

        }
    }
}
