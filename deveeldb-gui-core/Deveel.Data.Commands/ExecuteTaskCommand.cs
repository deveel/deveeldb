using System;
using System.Windows.Forms;

namespace Deveel.Data.Commands {
	[CommandSmallImage("Deveel.Data.Images.lightning.png")]
	public sealed class ExecuteTaskCommand : Command {
		public ExecuteTaskCommand() 
			: base("&Execute", Keys.F5, "F5") {
		}

		public override bool Enabled {
			get {
				ITask task = HostWindow.ActiveChild as ITask;
				return (task == null ? false : !task.IsBusy);
			}
		}

		public override void Execute() {
			ITask task = HostWindow.ActiveChild as ITask;
			if (task != null)
				task.Execute();
		}
	}
}