using flight.etl.app.Common;
using Microsoft.Extensions.Options;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Linq;
using System.Text.RegularExpressions;

namespace flight.etl.app.Services
{
    public class FlightEventValidationService
    {
        FlightDataSettings _flightDataSettings;
        Dictionary<string, JObject> _validators = new Dictionary<string, JObject>();

        public FlightEventValidationService(IOptions<FlightDataSettings> options)
        {
            _flightDataSettings = options.Value;

            try
            {
                var validatorDirectory = _flightDataSettings.ValidatorsDirectory;
                var baseDirectoryPath = AppDomain.CurrentDomain.BaseDirectory;

                foreach (string validatorJsonFile in Directory.EnumerateFiles(validatorDirectory))
                {
                    string contents = File.ReadAllText(validatorJsonFile);
                    var validationSchema = JObject.Parse(contents);

                    _validators.Add((string)validationSchema[Constants.EventData_Field_EventType], validationSchema);

                    Debug.WriteLine((string)validationSchema[Constants.EventData_Field_EventType] + " validation schema is registered ");
                }
            }
            catch (Exception e)
            {
                Debug.WriteLine(e.Message);
                throw new FileLoadException("Unable to load validators");
            }
        }

        public bool ValidateEvent(JObject eventData)
        {
            try
            {
                var eventType = eventData[Constants.EventData_Field_EventType].ToString();
                var validatorForEventType = _validators[eventType];

                JArray validationProperties = (JArray)validatorForEventType[Constants.Validation_schema_Field_Properties];

                int numberOfRequiredProperties = validationProperties.Where(x => (string)x["isRequired"] == "true").Count();

                if (numberOfRequiredProperties != eventData.Children().Count())
                {
                    throw new MinimumRequiredFieldsUnavailableException("Minimum number of required fields for event data are not present");
                }

                var validationErrors = new List<string>();
                foreach (JObject validationProperty in validationProperties)
                {
                    var eventPropertyValue = eventData[validationProperty[Constants.Validation_schema_Field_Properties_Name]].ToString();

                    if (ValidateEventPropertyByRegex(validationProperty[Constants.Validation_schema_Field_Properties_ValidationRegex].ToString(), eventPropertyValue))
                    {
                        validationErrors.Add(validationProperty[Constants.Validation_schema_Field_Properties_ErrorMessage].ToString());
                    }                    
                }

                if (validationErrors.Any())
                {
                    throw new ValidationErrorException(String.Join("\n", validationErrors.ToArray()));
                }
            }
            catch (Exception e)
            {
                Debug.WriteLine("Invalid event data " + eventData.ToString());
                Debug.WriteLine(e.Message);                
            }
                        
            return true;
        }

        public bool ValidateEventPropertyByRegex(string regex, string eventPropertyValue)
        {
            var match = Regex.Match(eventPropertyValue, regex, RegexOptions.IgnoreCase);
            return match.Success;
        }
    }
}
