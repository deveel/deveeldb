using System;

using Deveel.Commands;

namespace Deveel.Data.Commands {
	[Command("tables", ShortDescription = "lists the tabales of a dabatase")]
	[CommandSynopsis("tables [ for <schema> [ like <table-name> ] ]")]
	[CommandGroup("query")]
	internal class TablesCommand : ListUserObjectsCommand {	
	}
}