using System;

using Deveel.Data.Configuration;

namespace Deveel.Data.Protocol {
	public interface IClient : IDisposable {
		IConfiguration Configuration { get; }

		//TODO: Provide a method for listing all the database connection properties

		IDatabaseClient ConnectToDatabase(IConfiguration config);
	}
}
