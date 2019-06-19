using flight.etl.app.Common;
using flight.etl.app.Pipelines.Interface;
using flight.etl.app.Services;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace flight.etl.app.Pipelines
{
    public class TransformEventsProcess : IPipelineProcess
    {
        JArray _eventList;
        Dictionary<string, JArray> _curatedEventsGroupedByType = new Dictionary<string, JArray>();
        Dictionary<string, JArray> _exceptionEventsGroupedByType = new Dictionary<string, JArray>();

        FlightEventValidationService _flightEventValidationService;

        Tuple<Dictionary<string, JArray>, Dictionary<string, JArray>> _transformedEvents;

        public bool IsComplete { get; set; }

        public FlightDataSettings FlightDataSettings { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public long ProcessingTimeStamp { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public TransformEventsProcess(FlightEventValidationService flightEventValidationService)
        {
            _flightEventValidationService = flightEventValidationService;
        }

        public void Connect(IPipelineProcess next)
        {
            next.SetInput(_transformedEvents);
        }

        public void Process()
        {
            foreach (JObject eventData in _eventList.Children<JObject>())
            {
                try
                {
                    Debug.WriteLine(eventData.ToString());
                    ValidateEvent(eventData);
                }
                catch (Exception e)
                {
                    Debug.WriteLine(e.Message);
                }                
            }

            IsComplete = true;
        }

        public void SetInput<T>(T eventJsonArray)
        {
            _eventList = (JArray)(object)eventJsonArray;
        }

        void ValidateEvent(JObject eventData)
        {
            if (_flightEventValidationService.ValidateEvent(eventData))
            {
                AddToCuratedEventGroup(eventData);
            }
            else
            {
                AddToExceptionEventGroup(eventData);
            }
        }

        bool ValidateDepartureFile(JObject jsondata)
        {
            // validates the event and raises the exception
            return true;
        }

        bool ValidateArrivalFile(JObject jsondata)
        {
            // validates the event and raises the exception
            return true;
        }

        void AddToCuratedEventGroup(JObject eventData)
        {
            if (_curatedEventsGroupedByType.ContainsKey((string)eventData["eventType"]))
            {
                _curatedEventsGroupedByType[(string)eventData["eventType"]].Add(eventData);
            }
            else
            {
                _curatedEventsGroupedByType[(string)eventData["eventType"]] = new JArray { eventData };
            }
        }

        void AddToExceptionEventGroup(JObject eventData)
        {
            if (_exceptionEventsGroupedByType.ContainsKey((string)eventData["eventType"]))
            {
                _exceptionEventsGroupedByType[(string)eventData["eventType"]].Add(eventData);
            }
            else
            {
                _exceptionEventsGroupedByType[(string)eventData["eventType"]] = new JArray { eventData };
            }
        }
    }
}
