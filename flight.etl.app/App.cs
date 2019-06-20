using System.Diagnostics;
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
using System.Threading;
using System.Linq;

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
            var filesToProcess = Directory.EnumerateFiles(Path.Combine(_flightDataSettings.BaseDirectory, _flightDataSettings.InputDirectory));

            if (filesToProcess.Count() > 0)
            {
                _flightEventValidationService.LoadJsonValidators();
            }
            else
            {                
                return;
            }

            List<Task> taskList = new List<Task>(); 
            foreach (string currentFile in filesToProcess)
            {
                try
                {
                    taskList.Add(Task.Factory.StartNew((object file) =>
                    {
                        List<string> pipelineSummary = new List<string>();

                        try
                        {
                            var fileToProcess = (string)file;

                            pipelineSummary.Add("Batch Processing started of file :: " + fileToProcess);

                            var batchProcessTimer = Stopwatch.StartNew();

                            var fileProcessingTimeStamp = DateTime.Now.Ticks;

                            Pipeline flightEtlPipeline = new Pipeline();

                            flightEtlPipeline.Add(new ExtractFileProcess(pipelineSummary, _flightDataSettings, fileToProcess, _logger));
                            flightEtlPipeline.Add(new TransformEventsProcess(pipelineSummary, _flightEventValidationService, _logger));
                            flightEtlPipeline.Add(new LoadEventsToFileProcess(pipelineSummary, _flightDataSettings, fileProcessingTimeStamp, _logger));

                            pipelineSummary.Add("Batch Processing Summary of file :: " + fileToProcess);

                            flightEtlPipeline.Run();
                            batchProcessTimer.Stop();

                            pipelineSummary.Add("Time in milliseconds to process batch " + batchProcessTimer.ElapsedMilliseconds);

                            pipelineSummary.Add("Batch Processing ended");
                        }
                        catch (Exception e)
                        {
                            pipelineSummary.Add("Error:: " + e.Message);
                            pipelineSummary.Add("Error:: " + "Batch file processing terminated.");
                        }

                        _logger.LogInformation(string.Join(System.Environment.NewLine, pipelineSummary.ToArray()));
                    }, currentFile));                    
                }
                catch (Exception e)
                {
                    _logger.LogError(e.Message);
                    _logger.LogError("Batch file processing terminated.");
                }
            }

            Task.WaitAll(taskList.ToArray());
        }
    }
}
