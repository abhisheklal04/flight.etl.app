using flight.etl.app.Common;
using flight.etl.app.Pipelines.Interface;
using flight.etl.app.Services;
using Microsoft.Extensions.Logging;
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
        public List<string> _pipelineSummary { get; set; }

        object _input;
        JArray _eventList;
        Dictionary<string, JArray> _curatedEventsGroupedByType;
        Dictionary<string, JArray> _exceptionEventsGroupedByType;
        FlightEventValidationService _flightEventValidationService;
        Tuple<Dictionary<string, JArray>, Dictionary<string, JArray>> _transformedEvents;
        Dictionary<string, int> _distinctEventTypesFound = new Dictionary<string, int>();

        List<string> _failedEventsIds = new List<string>();

        ILogger _logger;
        public TransformEventsProcess(List<string> pipelineSummary, FlightEventValidationService flightEventValidationService, ILogger logger)
        {
            _logger = logger;
            _pipelineSummary = pipelineSummary;
            _flightEventValidationService = flightEventValidationService;
            _curatedEventsGroupedByType = new Dictionary<string, JArray>();
            _exceptionEventsGroupedByType = new Dictionary<string, JArray>();
            _transformedEvents = new Tuple<Dictionary<string, JArray>, Dictionary<string, JArray>>(_curatedEventsGroupedByType, _exceptionEventsGroupedByType);
        }

        public void SetInput(object input)
        {
            _input = input;
        }

        public void Connect(IPipelineProcess next)
        {
            next.SetInput(_transformedEvents);
        }

        public void Process()
        {
            _eventList = (JArray)_input;

            _logger.LogInformation("Validating and transforming event list");

            int eventDataItemIndex = 0;
            foreach (JObject eventData in _eventList.Children<JObject>())
            {
                eventDataItemIndex++;
                try
                {
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
                    _logger.LogError(e.Message);
                }
            }

            IsComplete = true;

            _pipelineSummary.Add("Number of distinct event types found in batch file: " + _distinctEventTypesFound.Keys.Count);
            foreach (var eventType in _distinctEventTypesFound)
            {
                _pipelineSummary.Add("No. of event for " + eventType.Key + "::" + eventType.Value);
            }

            if (_failedEventsIds.Count > 0)
            {
                _pipelineSummary.Add(
                    "Number of failed Validation events :: " +
                    _failedEventsIds.Count + System.Environment.NewLine +
                    "Events Failed :: " + System.Environment.NewLine +
                    string.Join(System.Environment.NewLine, _failedEventsIds.ToArray())
                );
            }
            else
            {
                _pipelineSummary.Add("All events succesfully passed validation.");
            }

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
