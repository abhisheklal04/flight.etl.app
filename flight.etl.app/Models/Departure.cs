using System;
using System.Collections.Generic;
using System.Text;

namespace flight.etl.app.Models
{
    public class Departure
    {
        public string EventType;
        public DateTime TimeStamp;
        public string Flight;
        public string Destination;
        public int Passengers;
    }
}
