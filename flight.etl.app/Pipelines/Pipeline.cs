using flight.etl.app.Pipelines.Interface;
using System;
using System.Collections.Generic;
using System.Text;

namespace flight.etl.app.Pipelines
{
    public class Pipeline
    {
        /// <summary>
        /// List of pipeline elements
        /// </summary>
        List<IPipelineProcess> pipeline = new List<IPipelineProcess>();

        /// <summary>
        /// Adds the element to the pipeline and links them.
        /// </summary>
        public void Add(IPipelineProcess anElement)
        {
            pipeline.Add(anElement);
            //if (pipeline.Count > 1)
            //    pipeline[pipeline.Count - 2].Connect(pipeline[pipeline.Count - 1]);
        }

        /// <summary>
        /// This is the main processing method. It runs the pipeline until all the 
        /// elements declare completion.
        /// </summary>
        public void Run()
        {
            bool jobCompleted = false;

            // Run the pipeline until the job is not completed
            while (!jobCompleted)
            {
                jobCompleted = true;
                for (int i = 0; i < pipeline.Count; i++)
                {
                    pipeline[i].Process();
                    if (i < pipeline.Count - 1)
                        pipeline[i].Connect(pipeline[i+1]);
                    jobCompleted = jobCompleted && pipeline[i].IsComplete;
                }
            }
        }
    }
}

