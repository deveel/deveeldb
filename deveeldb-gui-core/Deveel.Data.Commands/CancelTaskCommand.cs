using System;

namespace Deveel.Data.Commands {
	[CommandSmallImage("Deveel.Data.Images.stop.png")]
	public class CancelTaskCommand : Command {
		public CancelTaskCommand() 
			: base("&Cancel") {
		}

		public override bool Enabled {
			get {
				ITask task = HostWindow.ActiveChild as ITask;
				return (task == null ? false : task.IsBusy);
			}
		}

		public override void Execute() {
			if (!Enabled)
				return;

			ITask task = HostWindow.ActiveChild as ITask;
			if (task != null)
				task.Cancel();
		}
	}
}