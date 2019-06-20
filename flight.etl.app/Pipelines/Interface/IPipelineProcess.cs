using flight.etl.app.Common;
using System;
using System.Collections.Generic;
using System.IO.Pipes;
using System.Text;

namespace flight.etl.app.Pipelines.Interface
{
    public interface IPipelineProcess
    {        
        void SetInput(object input);
        
        void Connect(IPipelineProcess next);

        void Process();

        bool IsComplete
        {
            get;
        }

        List<string> _pipelineSummary { get; set; }
    }
}
