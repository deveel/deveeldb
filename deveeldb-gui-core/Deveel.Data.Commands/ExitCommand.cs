using System;

namespace Deveel.Data.Commands {
	[Command("Exit", "E&xit")]
	public sealed class ExitCommand : Command {
		public override void Execute() {
			HostWindow.Form.Close();
		}
	}
}