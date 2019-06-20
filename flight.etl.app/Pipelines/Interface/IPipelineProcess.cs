using flight.etl.app.Common;
using System;
using System.Collections.Generic;
using System.IO.Pipes;
using System.Text;

namespace flight.etl.app.Pipelines.Interface
{
    public interface IPipelineProcess
    {        
        void SetInput<T>(T input);
        
        void Connect(IPipelineProcess next);

        void Process();

        bool IsComplete
        {
            get;
        }

        FlightDataSettings FlightDataSettings { get; set; }

        long ProcessingTimeStamp { get; set; }

        List<string> PipelineSummary { get; set; }
    }
}
