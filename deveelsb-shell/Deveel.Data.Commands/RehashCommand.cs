using System;

using Deveel.Commands;
using Deveel.Data.Shell;

namespace Deveel.Data.Commands {
	[Command("rehash", ShortDescription = "rebuild the internal hash for tablename completion")]
	internal class RehashCommand : Command {
		public override CommandResultCode Execute(object context, string[] args) {
			SqlSession session = (SqlSession) context;

			try {
				session.RehashTableCompleter();
				return CommandResultCode.Success;
			} catch(Exception) {
				return CommandResultCode.ExecutionFailed;
			}
		}
	}
}