using System;

namespace Deveel.Data.Commands {
	public sealed class ExitCommand : Command {
		public ExitCommand() 
			: base("E&xit") {
		}

		public override void Execute() {
			HostWindow.Form.Close();
		}
	}
}