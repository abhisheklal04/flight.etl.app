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
        public bool IsComplete { get; set; }
        public FlightDataSettings FlightDataSettings { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public long ProcessingTimeStamp { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public List<string> PipelineSummary { get; set; }

        JArray _eventList;
        Dictionary<string, JArray> _curatedEventsGroupedByType;
        Dictionary<string, JArray> _exceptionEventsGroupedByType;
        FlightEventValidationService _flightEventValidationService;
        Tuple<Dictionary<string, JArray>, Dictionary<string, JArray>> _transformedEvents;
        Dictionary<string, int> _distinctEventTypesFound = new Dictionary<string, int>();

        List<string> _failedEventsIds = new List<string>();

        public TransformEventsProcess(List<string> pipelineSummary, FlightEventValidationService flightEventValidationService)
        {
            PipelineSummary = pipelineSummary;
            _flightEventValidationService = flightEventValidationService;
            _curatedEventsGroupedByType = new Dictionary<string, JArray>();
            _exceptionEventsGroupedByType = new Dictionary<string, JArray>();
            _transformedEvents = new Tuple<Dictionary<string, JArray>, Dictionary<string, JArray>>(_curatedEventsGroupedByType, _exceptionEventsGroupedByType);
        }

        public void Connect(IPipelineProcess next)
        {
            next.SetInput(_transformedEvents);
        }

        public void Process()
        {
            int eventDataItemIndex = 0;
            foreach (JObject eventData in _eventList.Children<JObject>())
            {
                eventDataItemIndex++;
                try
                {
                    Debug.WriteLine(eventData.ToString());

                    var incomingEventType = (string)eventData[Constants.EventData_Field_EventType];

                    if (_distinctEventTypesFound.ContainsKey(incomingEventType))
                    {
                        _distinctEventTypesFound[incomingEventType]++;
                    }
                    else
                    {
                        _distinctEventTypesFound.Add(incomingEventType, 1);
                    }

                    var validationResult = _flightEventValidationService.ValidateEvent(eventData);

                    if (validationResult == EventValidationResult.ValidationSuccess)
                    {
                        AddToCuratedEventGroup(eventData, incomingEventType);
                    }
                    else if (validationResult == EventValidationResult.ValidationFailed)
                    {
                        AddToExceptionEventGroup(eventData, incomingEventType);
                    }
                    else
                    {
                        AddToExceptionEventGroup(eventData, Constants.Unknown_EventType);
                    }

                    // logging number of failed events
                    if (validationResult != EventValidationResult.ValidationSuccess)
                    {
                        if (eventData.ContainsKey(Constants.EventData_Field_EventType) && eventData.ContainsKey(Constants.EventData_Field_Flight))
                        {
                            _failedEventsIds.Add("event at index : " + eventDataItemIndex.ToString() + " id: " + eventData[Constants.EventData_Field_EventType].ToString() + eventData[Constants.EventData_Field_Flight].ToString());
                        }
                        else
                        {
                            _failedEventsIds.Add("event at index : " + eventDataItemIndex.ToString() + " id : " + " Undefined");
                        }
                    }
                }
                catch (Exception e)
                {
                    Debug.WriteLine(e.Message);
                }
            }

            IsComplete = true;

            PipelineSummary.Add("Number of distinct event types found in batch file: " + _distinctEventTypesFound.Keys.Count);
            foreach (var eventType in _distinctEventTypesFound)
            {
                PipelineSummary.Add("No. of event for " + eventType.Key + "::" + eventType.Value);
            }

            PipelineSummary.Add("Number of failed Validation events :: " + _failedEventsIds.Count + " \n  Events Failed :: \n " + string.Join("\n", _failedEventsIds.ToArray()));
        }

        public void SetInput<T>(T eventJsonArray)
        {
            _eventList = (JArray)(object)eventJsonArray;
        }

        void AddToCuratedEventGroup(JObject eventData, string eventType)
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

        void AddToExceptionEventGroup(JObject eventData, string eventType)
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
