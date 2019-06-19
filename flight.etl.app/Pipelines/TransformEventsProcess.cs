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
        Dictionary<EventType, JArray> _curatedEventsGroupedByType = new Dictionary<EventType, JArray>();
        Dictionary<EventType, JArray> _exceptionEventsGroupedByType = new Dictionary<EventType, JArray>();

        FlightEventValidationService _flightEventValidationService;

        Tuple<Dictionary<EventType, JArray>, Dictionary<EventType, JArray>> _transformedEvents;

        public bool IsComplete { get; set; }

        public FlightDataSettings FlightDataSettings { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public long ProcessingTimeStamp { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        TransformEventsProcess(FlightEventValidationService flightEventValidationService)
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
            var eventType = eventData["eventType"].ToString();
            if (eventType == "Departure")
            {
                Debug.WriteLine("Departure data detected");
                try
                {
                    if (ValidateDepartureFile(eventData))
                    {

                    }
                }
                catch (Exception e)
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

        void AddToCuratedEventGroup(EventType eventType, JObject eventData)
        {
            if (_curatedEventsGroupedByType.ContainsKey(eventType))
            {
                _curatedEventsGroupedByType[eventType].Add(eventData);
            }
            else
            {
                _curatedEventsGroupedByType[eventType] = new JArray { eventData };
            }
        }

        void AddToExceptionEventGroup(EventType eventType, JObject eventData)
        {
            if (_exceptionEventsGroupedByType.ContainsKey(eventType))
            {
                _exceptionEventsGroupedByType[eventType].Add(eventData);
            }
            else
            {
                _exceptionEventsGroupedByType[eventType] = new JArray { eventData };
            }
        }
    }
}
