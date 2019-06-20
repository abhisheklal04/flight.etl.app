using System;
using System.Collections.Generic;
using System.Text;

namespace flight.etl.app.Common
{
    public enum EventValidationResult
    {
        ValidationSuccess,
        ValidationFailed,
        UnknownEventFound
    }
}
