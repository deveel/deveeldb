using System;

namespace Deveel.Data.Client.Commands {
	public interface ICommandEventHandler {
		void BeforeExecute();

		void AfterExecute();
	}
}
