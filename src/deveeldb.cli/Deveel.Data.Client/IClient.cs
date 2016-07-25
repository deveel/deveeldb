using System;
using System.Data;

namespace Deveel.Data.Client {
	public interface IClient : IInterruptable, IDisposable {
		bool IsConnected { get; }


		void Connect(string connectionString);

		void Disconnect();

		IDbCommand CreateDbCommand();
	}
}
