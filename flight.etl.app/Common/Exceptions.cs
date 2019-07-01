﻿using System;
using System.Collections.Generic;
using System.Text;

namespace flight.etl.app.Common
{
    public class InvalidAppSettingsException : Exception { public InvalidAppSettingsException(string message) : base(message) { } }
    public class InvalidJsonInputException : Exception { public InvalidJsonInputException(string message) : base(message) { } }
    public class RequiredException : Exception { public RequiredException(string message) : base(message) { } }
    public class ValidationErrorException : Exception { public ValidationErrorException(string message) : base(message) { } }
    public class EventTypeMissingException : Exception { public EventTypeMissingException(string message) : base(message) { } }
    public class MinimumRequiredFieldsUnavailableException : Exception { public MinimumRequiredFieldsUnavailableException(string message) : base(message) { } }
}
