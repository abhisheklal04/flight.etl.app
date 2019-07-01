using flight.etl.app.Common;
using flight.etl.app.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using Xunit;
using Moq;
using System.Diagnostics;
using Newtonsoft.Json.Linq;

namespace Tests.Services
{
    public class FlightEventValidationServiceTests : BaseTests
    {

        FlightEventValidationService GetServiceAndLoadValidators()
        {
            var mockLogger = new Mock<ILogger<FlightEventValidationService>>();
            var options = Options.Create(flightDataSettings);

            var service = new FlightEventValidationService(options, mockLogger.Object);
            service.LoadFlightEventJsonValidators();
            return service;
        }

        [Fact]
        public void Should_load_validators_from_flightdatasettings()
        {            
            try
            {
                GetServiceAndLoadValidators();
                Assert.True(true, "Validator file loaded.");
            }
            catch (Exception e)
            {
                Debug.WriteLine(e.Message);
                Assert.True(false, e.Message);
            }
        }

        [Fact]
        public void Should_return_validation_result_of_success_type_for_valid_event()
        {
            var service = GetServiceAndLoadValidators();
            var eventData = JObject.Parse(@"{
                                ""eventType"": ""Departure"",
                                ""timeStamp"": ""2017-11-27 09:43:56Z"",
                                ""flight"" : ""QA123"",
                                ""destination"": ""Brisbane"",
                                ""passengers"" : ""177""
                            }"); 
            var result = service.ValidateEvent(eventData);
            Assert.Equal(EventValidationResultType.ValidationSuccess, result.EventValidationResult);
        }

        [Fact]
        public void Should_return_validation_result_with_type_and_message_for_failed_event()
        {
            var service = GetServiceAndLoadValidators();
            var eventData = JObject.Parse(@"{
                                ""eventType"": ""Departure"",
                                ""timeStamp"": ""2017-11-2709:43:56Z"",
                                ""flight"" : ""QA123"",
                                ""destination"": ""Brisbane"",
                                ""passengers"" : ""177""
                            }");
            var result = service.ValidateEvent(eventData);
            Assert.Equal(EventValidationResultType.ValidationFailed, result.EventValidationResult);
        }

        [Fact]
        public void Should_fail_when_no_valid_event_type_found()
        {
            var service = GetServiceAndLoadValidators();
            var eventData = JObject.Parse(@"{
                                ""eventType"": ""Unknown"",
                                ""timeStamp"": ""2017-11-27 09:43:56Z"",
                                ""flight"" : ""QA123"",
                                ""destination"": ""Brisbane"",
                                ""passengers"" : ""177""
                            }");
            var result = service.ValidateEvent(eventData);
            Assert.Equal(EventValidationResultType.ValidationFailed, result.EventValidationResult);
        }

        [Fact]
        public void Should_fail_when_required_fields_are_not_present_in_event()
        {
            var service = GetServiceAndLoadValidators();
            var eventTypeMissing = JObject.Parse(@"{
                                ""timeStamp"": ""2017-11-2709:43:56Z"",
                                ""flight"" : ""QA123"",
                                ""destination"": ""Brisbane"",
                                ""passengers"" : ""177""
                            }");
            var result1 = service.ValidateEvent(eventTypeMissing);

            var eventTimestampMissing = JObject.Parse(@"{
                                ""eventType"": ""Departure"",
                                ""flight"" : ""QA123"",
                                ""destination"": ""Brisbane"",
                                ""passengers"" : ""177""
                            }");
            var result2 = service.ValidateEvent(eventTimestampMissing);

            var eventFlightNumberMissing = JObject.Parse(@"{
                                ""eventType"": ""Departure"",
                                ""timeStamp"": ""2017-11-2709:43:56Z"",
                                ""destination"": ""Brisbane"",
                                ""passengers"" : ""177""
                            }");
            var result3 = service.ValidateEvent(eventFlightNumberMissing);

            var eventPassengerMissing = JObject.Parse(@"{
                                ""eventType"": ""Departure"",
                                ""timeStamp"": ""2017-11-2709:43:56Z"",
                                ""flight"" : ""QA123"",
                                ""destination"": ""Brisbane"",
                            }");
            var result4 = service.ValidateEvent(eventPassengerMissing);

            Assert.Equal(EventValidationResultType.ValidationFailed, result1.EventValidationResult);
            Assert.Equal(EventValidationResultType.ValidationFailed, result2.EventValidationResult);
            Assert.Equal(EventValidationResultType.ValidationFailed, result3.EventValidationResult);
            Assert.Equal(EventValidationResultType.ValidationFailed, result4.EventValidationResult);
        }

        [Fact]
        public void Should_fail_when_fields_have_invalid_pattern_in_validators()
        {
            var service = GetServiceAndLoadValidators();
            var eventInvalidTimeStampPattern = JObject.Parse(@"{
                                ""eventType"": ""Dparture"",
                                ""timeStamp"": ""2017-11-27T09:43:56Z"",
                                ""flight"" : ""QA123"",
                                ""destination"": ""Brisbane"",
                                ""passengers"" : ""177""
                            }");
            var result = service.ValidateEvent(eventInvalidTimeStampPattern);
            Assert.Equal(EventValidationResultType.ValidationFailed, result.EventValidationResult);
        }

        [Fact]
        public void Should_return_failed_messages_for_each_invalid_event_property()
        {
            var service = GetServiceAndLoadValidators();
            var eventInvalidTimeStampAndFlight = JObject.Parse(@"{
                                ""eventType"": ""Dparture"",
                                ""timeStamp"": ""2017-11-27T09:43:56Z"",
                                ""flight"" : """",
                                ""destination"": ""Brisbane"",
                                ""passengers"" : ""177""
                            }");
            var result = service.ValidateEvent(eventInvalidTimeStampAndFlight);
            Assert.Equal(EventValidationResultType.ValidationFailed, result.EventValidationResult);
            Assert.Contains("timeStamp", result.ErrorMessages);
            Assert.Contains("flight", result.ErrorMessages);
        }
    }
}
