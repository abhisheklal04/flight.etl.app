using flight.etl.app.Common;
using flight.etl.app.Pipelines.Interface;
using Microsoft.Extensions.Logging;
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

        public FlightDataSettings _flightDataSettings { get; set; }
        public long _processingTimeStamp { get; set; }
        public List<string> _pipelineSummary { get; set; }

        object _input;

        ILogger _logger;

        public LoadEventsToFileProcess(List<string> pipelineSummary, FlightDataSettings flightDataSettings, long processingTimeStamp, ILogger logger)
        {
            _logger = logger;
            _processingTimeStamp = processingTimeStamp != 0 ? processingTimeStamp : throw new Exception("Processing timestamp has not been set");
            _flightDataSettings = flightDataSettings ?? throw new Exception("Flight Data directory Settings has not been set");
            _pipelineSummary = pipelineSummary;
        }

        public void SetInput(object input)
        {
            _input = input;
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
                _logger.LogInformation("Events has been succesfully saved to respective curated and exception folders.");
            }
            catch (Exception e)
            {
                _logger.LogError(e.Message);
                _logger.LogError("Failed to load events to respective curated and exception folder.");
            }
        }

        void SaveGroupedEventsToFiles()
        {
            var _transformedEvents = (Tuple<Dictionary<string, JArray>, Dictionary<string, JArray>>)_input;

            _curatedEventsGroupedByType = _transformedEvents.Item1;
            _exceptionEventsGroupedByType = _transformedEvents.Item2;
            // writing curated event file
            foreach (var eventType in _curatedEventsGroupedByType.Keys)
            {
                var eventFileName = eventType + "-" + _processingTimeStamp + ".json";

                try
                {
                    var eventFilePath = Path.Combine(_flightDataSettings.BaseDirectory, _flightDataSettings.CuratedDirectory + "/" + eventType + "/" + eventFileName);
                    File.WriteAllText(eventFilePath, _curatedEventsGroupedByType[eventType].ToString());                    
                }
                catch (Exception e)
                {
                    _logger.LogError("Unable to write file " + eventFileName);
                    _logger.LogError(e.Message);
                }                
            }

            // writing exception event file
            foreach (var eventType in _exceptionEventsGroupedByType.Keys)
            {
                var eventFileName = eventType + "-" + _processingTimeStamp + ".json";
                try
                {                   
                    var eventFilePath = Path.Combine(_flightDataSettings.BaseDirectory, _flightDataSettings.ExceptionDirectory + "/" + eventType + "/" + eventFileName);
                    File.WriteAllText(eventFilePath, _exceptionEventsGroupedByType[eventType].ToString());
                }
                catch (Exception e)
                {
                    _logger.LogError("Unable to write file " + eventFileName);
                    _logger.LogError(e.Message);
                }
            }
        }
    }
}
