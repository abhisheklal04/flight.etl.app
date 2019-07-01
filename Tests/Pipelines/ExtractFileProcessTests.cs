using flight.etl.app.Common;
using flight.etl.app.Pipelines;
using flight.etl.app.Pipelines.Interface;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Xunit;

namespace Tests.Pipelines
{
    public class ExtractFileProcessTests : BaseTests
    {
        string GetValidFileFromInputDirectory()
        {
            var inputDirectory = Path.Combine(flightDataSettings.BaseDirectory, flightDataSettings.InputDirectory);
            var files = Directory.EnumerateFiles(inputDirectory, "valid.json");
            if (files.Any())
            {
                return files.FirstOrDefault();
            }

            return null;
        }

        string GetInValidJsonFileFromInputDirectory()
        {
            var inputDirectory = Path.Combine(flightDataSettings.BaseDirectory, flightDataSettings.InputDirectory);
            var files = Directory.EnumerateFiles(inputDirectory, "invalidFormat.json");
            if (files.Any())
            {
                return files.FirstOrDefault();
            }

            return null;
        }

        ExtractFileProcess GetProcess(string fileName)
        {
            var mockLogger = new Mock<ILogger<ExtractFileProcess>>();
            var pipelineSummary = new List<string>();
            return new ExtractFileProcess(pipelineSummary, flightDataSettings, fileName, mockLogger.Object);
        }

        [Fact]
        public void should_throw_exception_when_flightDataSettings_is_unavailable()
        {
            FlightDataSettings flightDataSettingsAsNull = null;
            var mockLogger = new Mock<ILogger<ExtractFileProcess>>();
            var pipelineSummary = new List<string>();
            try
            {
                var process = new ExtractFileProcess(pipelineSummary, flightDataSettingsAsNull, string.Empty, mockLogger.Object);
                Assert.True(false);
            }
            catch (InvalidAppSettingsException e)
            {
                Assert.True(true);
            }
        }

        [Fact]
        public void should_throw_exception_if_file_input_is_not_provided()
        {
            var mockLogger = new Mock<ILogger<ExtractFileProcess>>();
            var pipelineSummary = new List<string>();
            try
            {
                var process = new ExtractFileProcess(pipelineSummary, flightDataSettings, string.Empty, mockLogger.Object);
                Assert.True(false);
            }
            catch (FileNotFoundException e)
            {
                Assert.True(true);
            }
        }

        [Fact]
        public void should_set_file_path_as_input()
        {
            var mockLogger = new Mock<ILogger<ExtractFileProcess>>();
            var pipelineSummary = new List<string>();
            try
            {
                var file = GetValidFileFromInputDirectory();
                var process = new ExtractFileProcess(pipelineSummary, flightDataSettings, file, mockLogger.Object);
                Assert.True(true);
            }
            catch (FileNotFoundException e)
            {
                Assert.True(false);
            }
        }

        [Fact]
        public void should_connect_next_pipeline_by_setting_its_input_with_extracted_event_data_list()
        {
            var mockLogger = new Mock<ILogger<ExtractFileProcess>>();
            var pipelineSummary = new List<string>();
            var file = GetValidFileFromInputDirectory();
            var process = new ExtractFileProcess(pipelineSummary, flightDataSettings, file, mockLogger.Object);
            var mockNextPipeline = new Mock<IPipelineProcess>();
            process.Connect(mockNextPipeline.Object);
            mockNextPipeline.Verify(c => c.SetInput(It.IsAny<object>()), Times.Once());
        }

        [Fact]
        public void should_extract_file_data_and_moves_it_to_raw_directory()
        {
            var mockLogger = new Mock<ILogger<ExtractFileProcess>>();
            var pipelineSummary = new List<string>();
            var file = GetValidFileFromInputDirectory();
            var process = new ExtractFileProcess(pipelineSummary, flightDataSettings, file, mockLogger.Object);
            var mockNextPipeline = new Mock<IPipelineProcess>();
            process.Connect(mockNextPipeline.Object);
            process.Process();
            Assert.True(process.IsComplete);
        }

        [Fact]
        public void should_throw_exception_when_invalid_json_data_is_provided()
        {
            var mockLogger = new Mock<ILogger<ExtractFileProcess>>();
            var pipelineSummary = new List<string>();
            var file = GetInValidJsonFileFromInputDirectory();
            var process = new ExtractFileProcess(pipelineSummary, flightDataSettings, file, mockLogger.Object);
            var mockNextPipeline = new Mock<IPipelineProcess>();
            process.Connect(mockNextPipeline.Object);
            Assert.Throws<InvalidJsonInputException>(() => process.Process());
            Assert.False(process.IsComplete);
        }

    }
}
