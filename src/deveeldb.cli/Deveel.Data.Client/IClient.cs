using System;
using System.Collections.Generic;

namespace Deveel.Data.Client {
	public interface IClient : IDisposable {
		bool IsConnected { get; }


		void Connect(string connectionString);

		void Disconnect();

		IEnumerable<IResult> ExecuteQuery(string commandText);
	}
}
