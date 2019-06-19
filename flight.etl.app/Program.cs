using System.IO;
using System.Threading.Tasks;
using flight.etl.app.Common;
using flight.etl.app.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace flight.etl.app
{
    public class Program
    {
        public static async Task Main(string[] args)
        {            
            var host = new HostBuilder()
                .ConfigureHostConfiguration(configHost =>
                {
                    configHost.SetBasePath(Directory.GetCurrentDirectory());
                    configHost.AddJsonFile("hostsettings.json", optional: true);
                    configHost.AddEnvironmentVariables(prefix: "PREFIX_");
                    configHost.AddCommandLine(args);
                })
                .ConfigureAppConfiguration((hostContext, configApp) =>
                {
                    configApp.AddJsonFile("appsettings.json", optional: true);
                    configApp.AddJsonFile(
                        $"appsettings.{hostContext.HostingEnvironment.EnvironmentName}.json",
                        optional: true);
                    configApp.AddEnvironmentVariables(prefix: "PREFIX_");
                    configApp.AddCommandLine(args);
                })                
                .ConfigureLogging((hostContext, logging) =>
                {
                    logging.AddConfiguration(hostContext.Configuration.GetSection("Logging"));
                    logging.AddConsole();
                    logging.AddDebug();
                })
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddOptions();

                    var flightData = hostContext.Configuration.GetSection("FlightData");
                    services.Configure<FlightDataSettings>(flightData);

                    // add services                                       
                    
                    services.AddHostedService<LifetimeEventsHostedService>();
                    //services.AddHostedService<TimedHostedService>();

                    services.AddTransient<App>();
                    services.AddSingleton<FlightEventValidationService>();
                })
                .UseConsoleLifetime()
                .Build();

            // adding file logging
            var loggerFactory = host.Services.GetRequiredService<ILoggerFactory>();
            loggerFactory.AddFile("Logs/mylog-{Date}.txt");

            await host.RunAsync();
        }

        
    }

}
