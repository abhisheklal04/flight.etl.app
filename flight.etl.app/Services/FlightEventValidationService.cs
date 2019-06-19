using flight.etl.app.Common;
using Microsoft.Extensions.Options;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace flight.etl.app.Services
{
    public class FlightEventValidationService
    {
        FlightDataSettings _flightDataSettings;
        Dictionary<string, JObject> _validators;

        public FlightEventValidationService(IOptions<FlightDataSettings> options)
        {
            _flightDataSettings = options.Value;
            var validatorDirectory = Path.Combine(_flightDataSettings.BaseDirectory, _flightDataSettings.ValidatorsDirectory);
            foreach (string validatorJsonFile in Directory.EnumerateFiles(validatorDirectory))
            {
                string contents = File.ReadAllText(validatorJsonFile);
                var validationSchema = JObject.Parse(contents);

                _validators.Add((string)validationSchema["eventType"], validationSchema);
            }
        }

        public bool ValidateEvent(JObject eventData)
        {
            return true;
        }
    }
}
