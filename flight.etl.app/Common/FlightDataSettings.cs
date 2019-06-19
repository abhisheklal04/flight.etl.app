using System;
using System.Collections.Generic;
using System.Text;

namespace flight.etl.app.Common
{
    public class FlightDataSettings
    {
        public string BaseDirectory { get; set; }
        public string InputDirectory { get; set; }
        public string RawDirectory { get; set; }
        public string ExceptionDirectory { get; set; }
        public string CuratedDirectory { get; set; }
        public string ValidatorsDirectory { get; set; }
    }
}
