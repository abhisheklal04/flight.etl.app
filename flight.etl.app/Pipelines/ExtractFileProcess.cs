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

        public ExtractFileProcess(FlightDataSettings flightDataSettings, string filePath)
        {
            FlightDataSettings = FlightDataSettings;
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
