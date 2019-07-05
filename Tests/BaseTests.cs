using flight.etl.app.Common;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Tests
{
    public class BaseTests
    {
        public FlightDataSettings flightDataSettings;

        public BaseTests()
        {
            flightDataSettings = CreateFakeFlightDataSettings();
            CreateTestDirectories();
            CopyTestInputFilesToInputDirectory();
        }

        public FlightDataSettings CreateFakeFlightDataSettings()
        {
            return new FlightDataSettings()
            {
                BaseDirectory = "TestFiles",
                InputDirectory = "01 - Input",
                RawDirectory = "02 - RAW",
                CuratedDirectory = "03 - Exception",
                ExceptionDirectory = "04 - Curated",
                ValidatorsDirectory = "Validators"
            };
        }

        public void CreateTestDirectories()
        {
            Directory.CreateDirectory(flightDataSettings.BaseDirectory);
            Directory.CreateDirectory(Path.Combine(flightDataSettings.BaseDirectory, flightDataSettings.InputDirectory));
            Directory.CreateDirectory(Path.Combine(flightDataSettings.BaseDirectory, flightDataSettings.RawDirectory));
            Directory.CreateDirectory(Path.Combine(flightDataSettings.BaseDirectory, flightDataSettings.CuratedDirectory));
            Directory.CreateDirectory(Path.Combine(flightDataSettings.BaseDirectory, flightDataSettings.ExceptionDirectory));
            Directory.CreateDirectory(flightDataSettings.ValidatorsDirectory);
        }

        public void CopyTestInputFilesToInputDirectory()
        {
            foreach (var file in Directory.EnumerateFiles("TestFiles", "*.json"))
            {
                var fileName = Path.GetFileName(file);
                File.Copy(file, Path.Combine(flightDataSettings.BaseDirectory, flightDataSettings.InputDirectory + "/" + fileName), true);
            }
        }

        public string GetValidFileFromInputDirectory()
        {
            var inputDirectory = Path.Combine(flightDataSettings.BaseDirectory, flightDataSettings.InputDirectory);
            var files = Directory.EnumerateFiles(inputDirectory, "valid.json");
            if (files.Any())
            {
                return files.FirstOrDefault();
            }

            return null;
        }

        public string GetInValidJsonFileFromInputDirectory()
        {
            var inputDirectory = Path.Combine(flightDataSettings.BaseDirectory, flightDataSettings.InputDirectory);
            var files = Directory.EnumerateFiles(inputDirectory, "invalidFormat.json");
            if (files.Any())
            {
                return files.FirstOrDefault();
            }

            return null;
        }

    }
}
