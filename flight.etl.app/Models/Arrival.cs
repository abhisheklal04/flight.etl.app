using System;
using System.Collections.Generic;
using System.Text;

namespace flight.etl.app.Models
{
    public class Arrival
    {
        public string EventType;
        public DateTime TimeStamp;
        public string Flight;
        public string Delayed;
        public int Passengers;
    }
}
