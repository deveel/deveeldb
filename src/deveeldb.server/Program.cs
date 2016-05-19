using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Deveel.Data;
using Deveel.Data.Configuration;
using Deveel.Data.Server;

using Topshelf;

namespace deveel_daemon {
	class Program {
		static int Main(string[] args) {
			var configFile = args != null && args.Length > 0 ? args[0] : null;
			var config = GetConfiguration(configFile);
			var handler = CreateHandler(config);

			return (int) HostFactory.Run(host => {
				host.Service<TcpService>(service => {
					service.ConstructUsing(() => new TcpService(config, handler));
					service.WhenStarted(s => s.Start());
					service.WhenStopped(s => s.Stop());
					service.WhenShutdown(s => s.Shutdown());
				});

				host.UseAssemblyInfoForServiceInfo();
			});
		}

		private static IDatabaseHandler CreateHandler(IConfiguration config) {
			var builder = new SystemBuilder(config);

			// TODO: load custom modules and other features ...

			return builder.BuildSystem();
		}

		private static IConfiguration GetConfiguration(string filePath) {
			throw new NotImplementedException();
		}
	}
}
