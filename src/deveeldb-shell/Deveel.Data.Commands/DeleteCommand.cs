using System;

using Deveel.Commands;

namespace Deveel.Data.Commands {
	[Command("delete", ShortDescription = "deletes data from a table")]
	[CommandSynopsis("delete from <table> [ where <where-clause> ]")]
	internal class DeleteCommand : SqlCommand {
		protected override bool IsUpdateCommand {
			get { return true; }
		}
	}
}