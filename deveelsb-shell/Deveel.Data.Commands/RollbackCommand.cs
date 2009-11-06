using System;

using Deveel.Commands;

namespace Deveel.Data.Commands {
	[Command("rollback", RequiresContext = true, ShortDescription = "rollbacks a current running transaction.")]
	internal class RollbackCommand : SqlCommand {
		protected override bool IsUpdateCommand {
			get { return true; }
		}
	}
}