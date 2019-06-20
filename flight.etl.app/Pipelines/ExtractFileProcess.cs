using flight.etl.app.Common;
using flight.etl.app.Pipelines.Interface;
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

        public FlightDataSettings FlightDataSettings { get; set; }
        public long ProcessingTimeStamp { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public List<string> PipelineSummary { get; set; }

        public ExtractFileProcess(List<string> pipelineSummary, FlightDataSettings flightDataSettings, string filePath)
        {
            PipelineSummary = PipelineSummary;

            FlightDataSettings = flightDataSettings ?? throw new Exception("Flight Data directory Settings has not been set");

            if (filePath == null)
            {
                throw new Exception("no file available to to start extract process");
            }

            SetInput(filePath);            
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

                var movedFilePath = Path.Combine(FlightDataSettings.BaseDirectory, FlightDataSettings.RawDirectory + "/" + fileName);
                Directory.Move(_filePath, movedFilePath);

                string contents = File.ReadAllText(movedFilePath);

                _eventList = ParseEventJsonData(contents);
                IsComplete = true;
                PipelineSummary.Add("Number of event found in batch file: " + fileName);
            }
            catch (Exception e)
            {
                Debug.WriteLine(e.Message);
            }            
        }

        public void SetInput<T>(T filePath)
        {
            _filePath = (string)(object)filePath;
        }

        JArray ParseEventJsonData(string contents)
        {
            return JArray.Parse(contents);            
        }
    }
}
