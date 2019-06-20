using System;
using System.Collections.Generic;
using System.Text;

namespace flight.etl.app.Common
{
    public enum EventValidationResultType
    {
        ValidationSuccess,
        ValidationFailed,
        UnknownEventFound
    }
}
