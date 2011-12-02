using System;

using Deveel.Commands;

namespace Deveel.Data.Commands {
	[Command("select", RequiresContext = true, ShortDescription = "selects data from table")]
	[CommandSynopsis("select <columns> from <table[s]> [ where <where-clause>] [ order by <columns> ] ...")]
	internal class SelectCommand : SqlCommand {
		protected override bool IsUpdateCommand {
			get { return false; }
		}
	}
}