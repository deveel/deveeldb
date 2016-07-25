using System;

using Deveel.Data.Client.Commands;

namespace Deveel.Data.Client {
	public interface IApplication : IInterruptable {
		ICommandRegistry CommandRegistry { get; }


		void SetPrompt(string prompt);

		LineStatus ExecuteLine(string line);

		void Execute();
	}
}
