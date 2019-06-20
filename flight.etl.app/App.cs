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

namespace flight.etl.app
{
    public class App
    {   
        static readonly Dictionary<EventType, JArray> EventsGroupedByType = new Dictionary<EventType, JArray>();
        FlightDataSettings _flightDataSettings;
        FlightEventValidationService _flightEventValidationService;

        public App(IOptions<FlightDataSettings> options, FlightEventValidationService flightEventValidationService)
        {
            _flightDataSettings = options.Value;
            _flightEventValidationService = flightEventValidationService;
        }

        public void StartBatchProcess()
        {
            foreach (string currentFile in Directory.EnumerateFiles(Path.Combine(_flightDataSettings.BaseDirectory, _flightDataSettings.InputDirectory)))
            {
                var batchProcessTimer = Stopwatch.StartNew();
                
                var fileProcessingTimeStamp = DateTime.Now.Ticks;
                List<string> pipelineSummary = new List<string>();

                Pipeline flightEtlPipeline = new Pipeline();

                flightEtlPipeline.Add(new ExtractFileProcess(pipelineSummary, _flightDataSettings, currentFile));
                flightEtlPipeline.Add(new TransformEventsProcess(pipelineSummary, _flightEventValidationService));
                flightEtlPipeline.Add(new LoadEventsToFileProcess(pipelineSummary, _flightDataSettings, fileProcessingTimeStamp));

                flightEtlPipeline.Run();

                batchProcessTimer.Stop();

                pipelineSummary.Add("Time to process batch " + batchProcessTimer.ElapsedMilliseconds);

                Debug.Write("Batch Processing Summary of file :: " + currentFile);
                Debug.WriteLine(string.Join('\n', pipelineSummary.ToArray()));
            }
        }
    }
}
