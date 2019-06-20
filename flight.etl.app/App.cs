﻿using System.Diagnostics;
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
using Microsoft.Extensions.Logging;

namespace flight.etl.app
{
    public class App
    {   
        static readonly Dictionary<EventType, JArray> EventsGroupedByType = new Dictionary<EventType, JArray>();
        FlightDataSettings _flightDataSettings;
        FlightEventValidationService _flightEventValidationService;
        ILogger _logger;

        public App(IOptions<FlightDataSettings> options, FlightEventValidationService flightEventValidationService, ILogger<App> logger)
        {
            _flightDataSettings = options.Value;
            _flightEventValidationService = flightEventValidationService;
            _logger = logger;
        }

        public void StartBatchProcess()
        {
            foreach (string currentFile in Directory.EnumerateFiles(Path.Combine(_flightDataSettings.BaseDirectory, _flightDataSettings.InputDirectory)))
            {
                try
                {
                    _logger.LogInformation("Batch Processing started of file :: " + currentFile);

                    var batchProcessTimer = Stopwatch.StartNew();

                    var fileProcessingTimeStamp = DateTime.Now.Ticks;
                    List<string> pipelineSummary = new List<string>();

                    Pipeline flightEtlPipeline = new Pipeline();

                    flightEtlPipeline.Add(new ExtractFileProcess(pipelineSummary, _flightDataSettings, currentFile, _logger));
                    flightEtlPipeline.Add(new TransformEventsProcess(pipelineSummary, _flightEventValidationService, _logger));
                    flightEtlPipeline.Add(new LoadEventsToFileProcess(pipelineSummary, _flightDataSettings, fileProcessingTimeStamp, _logger));

                    flightEtlPipeline.Run();

                    batchProcessTimer.Stop();

                    pipelineSummary.Add("Time in milliseconds to process batch " + batchProcessTimer.ElapsedMilliseconds);

                    _logger.LogInformation("Batch Processing Summary of file :: " + currentFile);
                    _logger.LogInformation(string.Join(System.Environment.NewLine, pipelineSummary.ToArray()));

                    _logger.LogInformation("Batch Processing ended");
                }
                catch (Exception e)
                {
                    _logger.LogError(e.Message);
                    _logger.LogError("Batch file processing terminated.");
                }
            }
        }
    }
}
