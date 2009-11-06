using System;
using System.Text;

using Deveel.Commands;
using Deveel.Data.Client;
using Deveel.Data.Shell;
using Deveel.Shell;

namespace Deveel.Data.Commands {
	internal abstract class ListUserObjectsCommand : Command {
		public override bool RequiresContext {
			get { return true; }
		}

		public override CommandResultCode Execute(object context, string[] args) {
			SqlSession session = (SqlSession) context;

			string schemaName = null;
			string tableName = null;

			if (args.Length > 0) {
				if (args.Length < 2)
					return CommandResultCode.SyntaxError;

				if (String.Compare(args[0], "for", true) != 0)
					return CommandResultCode.SyntaxError;

				schemaName = args[1];

				if (args.Length > 2) {
					if (String.Compare(args[2], "like", true) != 0)
						return CommandResultCode.SyntaxError;

					tableName = args[3];
				}
			}

			string[] types = null;
			if (Name.Equals("views")) {
				types = new string[] { "VIEW" };
			} else if (Name.Equals("tables")) {
				types = new string[] { "TABLE", "SYSTEM TABLE" };
			}

			try {
				DeveelDbDataReader reader = GetTables(session, schemaName, tableName, types);
				int[] tableDispCols = { 1, 2, 3, 4 };
				ResultSetRenderer renderer = new ResultSetRenderer(reader, "|", true, true, 10000,
				                                                   OutputDevice.Out, tableDispCols);


                renderer.DisplayColumns[2].AutoWrap = 78;
				int tables = renderer.Execute();

				if (tables > 0) {
					Application.MessageDevice.WriteLine(tables + " " + Name + " found.");
					if (renderer.LimitReached)
						Application.MessageDevice.WriteLine("..and probably more; reached display limit");
				}
			} catch(Exception e) {
				OutputDevice.Message.WriteLine(e.Message);
				return CommandResultCode.ExecutionFailed;
			}

			return CommandResultCode.Success;
		}

		private static DeveelDbDataReader GetTables(SqlSession session, string schema, string table, string[] types) {
			if (table == null)
				table = "%";
			if (schema == null)
				schema = "%";

			// The 'types' argument
			String type_part = "";
			int type_size = 0;
			if (types.Length > 0) {
				StringBuilder buf = new StringBuilder();
				buf.Append("      AND \"TABLE_TYPE\" IN ( ");
				for (int i = 0; i < types.Length - 1; ++i) {
					buf.Append("?, ");
				}
				buf.Append("? ) \n");
				type_size = types.Length;
				type_part = buf.ToString();
			}

			// Create the statement

			DeveelDbCommand command = session.Connection.CreateCommand("   SELECT * \n" +
			                                                           "     FROM \"INFORMATION_SCHEMA.TABLES\" \n" +
			                                                           "    WHERE \"TABLE_SCHEMA\" LIKE ? \n" +
			                                                           "      AND \"TABLE_NAME\" LIKE ? \n" +
			                                                           type_part +
			                                                           " ORDER BY \"TABLE_TYPE\", \"TABLE_SCHEMA\", \"TABLE_NAME\" \n");
			command.Parameters.Add(schema);
			command.Parameters.Add(table);
			if (type_size > 0) {
				for (int i = 0; i < type_size; ++i)
					command.Parameters.Add(types[i]);
			}

			command.Prepare();

			return command.ExecuteReader();
		}
	}
}