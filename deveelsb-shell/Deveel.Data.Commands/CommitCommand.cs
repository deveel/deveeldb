using System;

using Deveel.Commands;

namespace Deveel.Data.Commands {
	[Command("commit", RequiresContext = true, ShortDescription = "commits the current transaction")]
	internal class CommitCommand : SqlCommand {
	}
}