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
        string _fileToExtractEventData;
        JArray _eventList;

        public bool IsComplete { get; private set; }

        FlightDataSettings _flightDataSettings { get; set; }

        public List<string> _pipelineSummary { get; set; }

        ILogger _logger;

        public ExtractFileProcess(List<string> pipelineSummary, FlightDataSettings flightDataSettings, string fileToExtractEventData, ILogger logger)
        {
            _logger = logger;

            _pipelineSummary = pipelineSummary;

            _flightDataSettings = flightDataSettings ?? throw new InvalidAppSettingsException("Flight Data directory Settings has not been set");

            if (string.IsNullOrEmpty(fileToExtractEventData))
            {
                throw new FileNotFoundException("no file available to start extract process");
            }

            SetInput(fileToExtractEventData);            
        }

        public void SetInput(object fileToExtractEventData)
        {
            _fileToExtractEventData = (string)fileToExtractEventData;

            if (string.IsNullOrEmpty(_fileToExtractEventData))
            {
                throw new FileNotFoundException("no file available to start extract process");
            }
        }

        public void Connect(IPipelineProcess next)
        {
            next.SetInput(_eventList);
        }

        public void Process()
        {
            try
            {                
                var fileName = Path.GetFileName(_fileToExtractEventData);

                //string contents = File.ReadAllText(_filePath);

                var movedToRawDirectory = Path.Combine(_flightDataSettings.BaseDirectory, _flightDataSettings.RawDirectory);
                Directory.CreateDirectory(movedToRawDirectory);

                File.Copy(_fileToExtractEventData, movedToRawDirectory + "/" + fileName, true);
                File.Delete(_fileToExtractEventData);

                _pipelineSummary.Add("Batch file '" + fileName + "' moved to RAW directory");

                _pipelineSummary.Add("Reading batch file");

                string contents = File.ReadAllText(movedToRawDirectory + "/" + fileName);

                try
                {
                    _eventList = JArray.Parse(contents);
                }
                catch (Exception e)
                {
                    throw new InvalidJsonInputException("Invalid Json input file format detected");
                }                

                IsComplete = true;

                _pipelineSummary.Add("Total Number of events found in batch file: " + _eventList.Count);
            }
            catch (Exception e)
            {
                throw e;
            }                     
        }
    }
}
