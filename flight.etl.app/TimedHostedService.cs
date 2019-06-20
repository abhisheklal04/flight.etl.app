using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace flight.etl.app
{
    internal class TimedHostedService : IHostedService, IDisposable
    {
        private readonly ILogger _logger;
        private Timer _timer;
        private App _app;
        private IConfiguration _configuration;


        public TimedHostedService(ILogger<TimedHostedService> logger, App app, IConfiguration configuration)
        {            
            _logger = logger;
            _app = app;
            _configuration = configuration;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Timed Background Service is starting.");

            //_timer = new Timer(DoWork, null, TimeSpan.Zero,
            //    TimeSpan.FromSeconds(5));

            _timer = new Timer((g) =>
            {
                DoWork();
                var serviceRestartIntervalInSeconds = Convert.ToInt32(_configuration.GetSection("HostedService:BatchProcessingInvervalInSeconds").Value);
                _timer.Change(serviceRestartIntervalInSeconds * 1000, Timeout.Infinite);
            }, null, 0, Timeout.Infinite);

            return Task.CompletedTask;
        }

        private void DoWork()
        {   
            _app.StartBatchProcess();            
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Timed Background Service is stopping.");

            _timer?.Change(Timeout.Infinite, 0);

            return Task.CompletedTask;
        }

        public void Dispose()
        {
            _timer?.Dispose();
        }
    }
}
