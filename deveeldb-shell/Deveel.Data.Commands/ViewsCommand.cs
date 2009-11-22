using System;

using Deveel.Commands;

namespace Deveel.Data.Commands {
	[Command("views", ShortDescription = "lists the views in a database")]
	[CommandGroup("query")]
	[CommandSynopsis("views [ for <schema> [ like <table-name> ] ]")]
	internal class ViewsCommand : ListUserObjectsCommand {
	}
}