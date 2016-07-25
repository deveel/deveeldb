using System;

namespace Deveel.Data.Client.Commands {
	public sealed class CreateTableCommand : CommandBase {
		public CreateTableCommand() 
			: base("create-table") {
		}

		protected override object ExecuteCommand(string commandText) {
			throw new NotImplementedException();
		}
	}
}
