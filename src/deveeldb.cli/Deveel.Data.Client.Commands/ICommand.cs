using System;

namespace Deveel.Data.Client.Commands {
	public interface ICommand : IDisposable {
		CommandInfo CommandInfo { get; }

		bool Matches(string[] tokens);

		CommandResultCode Execute(string commandText, string[] args);

		void Cancel();
	}
}
