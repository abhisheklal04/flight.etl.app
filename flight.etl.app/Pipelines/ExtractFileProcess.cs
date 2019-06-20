﻿using flight.etl.app.Common;
using flight.etl.app.Pipelines.Interface;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Pipes;
using System.Text;

namespace flight.etl.app.Pipelines
{
    public class ExtractFileProcess : IPipelineProcess
    {
        string _filePath;
        JArray _eventList;

        public bool IsComplete { get; private set; }

        public FlightDataSettings _flightDataSettings { get; set; }
        public List<string> _pipelineSummary { get; set; }

        ILogger _logger;

        public ExtractFileProcess(List<string> pipelineSummary, FlightDataSettings flightDataSettings, string filePath, ILogger logger)
        {
            _logger = logger;

            _pipelineSummary = pipelineSummary;

            _flightDataSettings = flightDataSettings ?? throw new Exception("Flight Data directory Settings has not been set");

            if (filePath == null)
            {
                throw new Exception("no file available to to start extract process");
            }

            SetInput(filePath);            
        }

        public void SetInput(object filePath)
        {
            _filePath = (string)filePath;
        }

        public void Connect(IPipelineProcess next)
        {
            next.SetInput(_eventList);
        }

        public void Process()
        {
            try
            {                
                var fileName = Path.GetFileName(_filePath);

                string contents = File.ReadAllText(_filePath);

                _eventList = ParseEventJsonData(contents);

                //var movedFilePath = Path.Combine(FlightDataSettings.BaseDirectory, FlightDataSettings.RawDirectory + "/" + fileName);
                //Directory.Move(_filePath, movedFilePath);

                IsComplete = true;
                _pipelineSummary.Add("Total Number of events found in batch file: " + _eventList.Count);
            }
            catch (Exception e)
            {
                _logger.LogError(e.Message);
            }            
        }

        JArray ParseEventJsonData(string contents)
        {
            return JArray.Parse(contents);            
        }
    }
}
