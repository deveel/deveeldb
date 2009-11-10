using System;

namespace Deveel.Data.Commands {
	[CommandSmallImage("Deveel.Data.Images.cog.png")]
	public sealed class ShowOptionsCommand : Command {
		public ShowOptionsCommand()
			: base("Options") {
		}

		public override void Execute() {
			using (OptionsForm optionsForm = (OptionsForm) Services.Resolve(typeof(OptionsForm))) {
				optionsForm.ShowDialog(HostWindow.Form);
			}

		}
	}
}