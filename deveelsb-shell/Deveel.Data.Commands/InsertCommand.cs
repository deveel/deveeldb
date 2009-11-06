using System;

using Deveel.Commands;

namespace Deveel.Data.Commands {
	[Command("insert", ShortDescription = "inserts data into a table")]
	[CommandSynopsis("insert into <table> [(<columns>])] values (<values>)")]
	class InsertCommand : SqlCommand {
	}
}