using System;

using Deveel.Commands;

namespace Deveel.Data.Commands {
	[Command("update")]
	[CommandSynopsis("update <table> set <column>=<value>[,...] [ where <where-clause> ]")]
	[CommandSynopsis("update <table> [(<columns>)] VALUES (<values>)")]
	internal class UpdateCommand : SqlCommand {
		protected override bool IsUpdateCommand {
			get { return true; }
		}
	}
}