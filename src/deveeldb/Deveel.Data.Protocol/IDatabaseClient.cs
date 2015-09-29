using System;

using Deveel.Data.Configuration;

namespace Deveel.Data.Protocol {
	public interface IDatabaseClient : IDisposable {
		IClient Client { get; }

		IConfiguration Configuration { get; }

		bool IsBooted { get; }

		bool Exist { get; }


		IServerConnector Create(string adminUser, string adminPassword);

		IServerConnector Boot();

		IServerConnector Access();
	}
}
