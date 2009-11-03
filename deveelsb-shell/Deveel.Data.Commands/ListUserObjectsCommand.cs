using System;

using Deveel.Commands;
using Deveel.Data.Client;
using Deveel.Data.Shell;
using Deveel.Shell;

namespace Deveel.Data.Commands {
	internal abstract class ListUserObjectsCommand : Command, IInterruptable {
		private bool interrupted;

		protected abstract string ObjectType { get; }

		public override CommandResultCode Execute(object context, string[] args) {
			SqlSession session = (SqlSession) context;

			/*
			try {
				DeveelDbConnection conn = session.Connection;

				ResultSetRenderer renderer;
				DeveelDbDataReader rset;
				String objectType = ObjectType;
				int[] columnDef;
				string schemaName;

				if (objectType == "tables") {
					schemaName = DeveelDbMetadataSchemaNames.Tables;
				} else if (objectType == "views") {
					//TODO:
					return CommandResultCode.ExecutionFailed;
				}


						renderer = new ResultSetRenderer(rset, "|", true, true, 10000,
                                                 OutputDevice.Out, columnDef);
                renderer.getDisplayMetaData()[2].setAutoWrap(78);
			} catch(Exception e) {
				OutputDevice.Message.WriteLine(e.Message);
				return CommandResultCode.ExecutionFailed;
			}
			*/

			return CommandResultCode.ExecutionFailed;
		}

		public void Interrupt() {
		}
	}
}