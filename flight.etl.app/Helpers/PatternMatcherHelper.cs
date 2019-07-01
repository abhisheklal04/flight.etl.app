using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace flight.etl.app.Helpers
{
    public static class PatternMatcherHelper
    {
        public static bool ValidateEventPropertyByRegex(string regex, string eventPropertyValue)
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
