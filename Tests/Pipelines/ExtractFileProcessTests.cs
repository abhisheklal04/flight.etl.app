using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Tests.Pipelines
{
    public class ExtractFileProcessTests
    {
        [Fact]
        public void should_load_flight_settings_when_instantitating() { }

        [Fact]
        public void should_throw_exception_if_file_input_is_not_provided() { }

        [Fact]
        public void should_set_file_path_as_input() { }

        [Fact]
        public void should_connect_next_pipeline_by_setting_its_input_with_event_data_list() { }

        [Fact]
        public void should_extract_file_data_and_move_it_to_raw_directory() { }

    }
}
