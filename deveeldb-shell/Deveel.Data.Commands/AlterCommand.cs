using System;

using Deveel.Commands;

namespace Deveel.Data.Commands {
	[Command("alter", RequiresContext = true, ShortDescription = "alters the structure of a database object")]
	internal class AlterCommand : SqlCommand {
	}
}