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
using Microsoft.Extensions.Logging;

namespace flight.etl.app.Services
{
    public class FlightEventValidationService
    {
        FlightDataSettings _flightDataSettings;
        Dictionary<string, JObject> _validators = new Dictionary<string, JObject>();
        ILogger _logger;

        public FlightEventValidationService(IOptions<FlightDataSettings> options, ILogger<FlightEventValidationService> logger)
        {
            _logger = logger;
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

                    _logger.LogInformation((string)validationSchema[Constants.EventData_Field_EventType] + " validation schema is registered ");
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e.Message);
                throw new FileLoadException("Unable to load validators");
            }
        }

        public ValidationResult ValidateEvent(JObject eventData)
        {
            var validationResult = new ValidationResult();
            try
            {
                var eventType = eventData[Constants.EventData_Field_EventType].ToString();
                if (!_validators.ContainsKey(eventType))
                {
                    throw new EventTypeMissingException("Unknown event type found");
                }
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
                    var eventPropertyValue = eventData[validationProperty[Constants.Validation_schema_Field_Properties_Name].ToString()].ToString();

                    if (string.IsNullOrEmpty(eventPropertyValue) && validationProperty[Constants.Validation_schema_Field_Properties_IsRequired].ToString() == "true")
                    {
                        validationErrors.Add(validationProperty[Constants.Validation_schema_Field_Properties_ErrorMessage].ToString());
                    }

                    if (!ValidateEventPropertyByRegex(validationProperty[Constants.Validation_schema_Field_Properties_ValidationRegex].ToString(), eventPropertyValue))
                    {
                        validationErrors.Add(validationProperty[Constants.Validation_schema_Field_Properties_ErrorMessage].ToString());
                    }
                }

                if (validationErrors.Any())
                {
                    throw new ValidationErrorException(String.Join(System.Environment.NewLine, validationErrors.ToArray()));
                }

                validationResult.EventValidationResult = EventValidationResultType.ValidationSuccess;
            }
            catch (EventTypeMissingException e)
            {
                validationResult.ErrorMessages = e.Message + " :: for event data " + eventData.ToString();
                validationResult.EventValidationResult = EventValidationResultType.UnknownEventFound;
            }
            catch (Exception e)
            {
                validationResult.ErrorMessages = e.Message + " :: for event data " + eventData.ToString();
                validationResult.EventValidationResult = EventValidationResultType.ValidationFailed;
            }
                        
            return validationResult;
        }

        public bool ValidateEventPropertyByRegex(string regex, string eventPropertyValue)
        {
            if (string.IsNullOrEmpty(regex))
            {
                return true;
            }

            var match = Regex.Match(eventPropertyValue, regex, RegexOptions.IgnoreCase);
            return match.Success;
        }
    }
}
