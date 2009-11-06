using System;
using System.Data;
using System.IO;
using System.Text;

using Deveel.Commands;
using Deveel.Data.Commands;
using Deveel.Data.Shell;
using Deveel.Design;
using Deveel.Shell;

namespace Deveel.Data.Commands {
	internal class DumpExporter {
		public DumpExporter(DumpCommand command) {
			this.command = command;
		}

		private readonly DumpCommand command;

		private CommandResultCode dumpTable(SqlSession session,
					  String tabName,
					  String whereClause,
					  TextWriter dumpOut, String fileEncoding) {

			// asking for meta data is only possible with the correct
			// table name.
			bool correctName = true;
			if (tabName.StartsWith("\"")) {
				//tabName = stripQuotes(tabName);
				correctName = false;
			}

			// separate schama and table.
			String schema = null;
			int schemaDelim = tabName.IndexOf('.');
			if (schemaDelim > 0) {
				schema = tabName.Substring(0, schemaDelim);
				tabName = tabName.Substring(schemaDelim + 1);
			}

			if (correctName) {
				String alternative = session.CorrectTableName(tabName);
				if (alternative != null && !alternative.Equals(tabName)) {
					tabName = alternative;
					OutputDevice.Out.WriteLine("dumping table: '" + tabName + "' (corrected name)");
				}
			}
			TableDumpSource tableSource = new TableDumpSource(schema, tabName, !correctName, session);
			tableSource.setWhereClause(whereClause);
			return dumpTable(session, tableSource, dumpOut, fileEncoding);
		}

		private CommandResultCode dumpTable(SqlSession session,
							  IDumpSource dumpSource,
							  TextWriter dumpOut, String fileEncoding) {
			DateTime startTime = DateTime.Now;
			MetaProperty[] metaProps = dumpSource.MetaProperties;
			if (metaProps.Length == 0) {
				OutputDevice.Message.WriteLine("No fields in " + dumpSource.Description + " found.");
				return CommandResultCode.ExecutionFailed;
			}

			OutputDevice.Message.WriteLine("dump " + dumpSource.TableName + ":");

			dumpOut.WriteLine("(tabledump '" + dumpSource.TableName + "'");
			dumpOut.WriteLine("  (file-encoding '" + fileEncoding + "')");
			dumpOut.WriteLine("  (dump-version " + DumpCommand.DUMP_VERSION + " " + DumpCommand.DUMP_VERSION + ")");
			/*
			if (whereClause != null) {
				dumpOut.Write("  (where-clause ");
				QuoteString(dumpOut, whereClause);
				dumpOut.Write(")");
			}
			*/
			dumpOut.WriteLine("  (client-version '" + Deveel.Shell.ProductInfo.Current.Version.ToString(2) + "')");
			dumpOut.WriteLine("  (time '" + DateTime.Now.ToString() + "')");
			dumpOut.Write("  (database-info ");
			quoteString(dumpOut, session.DatabaseInfo);
			dumpOut.WriteLine(")");

			long expectedRows = dumpSource.ExpectedRows;
			dumpOut.WriteLine("  (estimated-rows '" + expectedRows + "')");

			dumpOut.Write("  (meta (");
			for (int i = 0; i < metaProps.Length; ++i) {
				MetaProperty p = metaProps[i];
				printWidth(dumpOut, p.fieldName, p.renderWidth(), i != 0);
			}
			dumpOut.WriteLine(")");
			dumpOut.Write("\t(");
			for (int i = 0; i < metaProps.Length; ++i) {
				MetaProperty p = metaProps[i];
				printWidth(dumpOut, p.typeName, p.renderWidth(), i != 0);
			}
			dumpOut.WriteLine("))");

			dumpOut.Write("  (data ");
			IDataReader rset = null;
			IDbCommand stmt = null;
			try {
				long rows = 0;
				ProgressWriter progressWriter = new ProgressWriter(expectedRows, OutputDevice.Message);
				rset = dumpSource.Reader;
				stmt = dumpSource.Command;
				bool isFirst = true;
				while (command.IsRunning && rset.Read()) {
					++rows;
					progressWriter.Update(rows);
					if (!isFirst)
						dumpOut.Write("\n\t");
					isFirst = false;
					dumpOut.Write("(");

					for (int i = 0; i < metaProps.Length; ++i) {
						DbTypes thisType = metaProps[i].Type;

						switch (thisType) {
							case DbTypes.DB_NUMERIC:
							case DbTypes.DB_NUMERIC_EXTENDED: {
								String val = rset.GetString(i);
								if (rset.IsDBNull(i))
									dumpOut.Write("NULL");
								else
									dumpOut.Write(val);
								break;
							}

							case DbTypes.DB_TIME: {
								DateTime val = rset.GetDateTime(i);
								if (rset.IsDBNull(i))
									dumpOut.Write("NULL");
								else {
									quoteString(dumpOut, val.ToString());
								}
								break;
							}

							case DbTypes.DB_STRING: {
									String val = rset.GetString(i);
									if (rset.IsDBNull(i))
										dumpOut.Write("NULL");
									else {
										quoteString(dumpOut, val);
									}
									break;
								}

							default:
								throw new ArgumentException("type " + MetaProperty.Types[thisType] + " not supported yet");
						}
						if (metaProps.Length > i)
							dumpOut.Write(",");
						else
							dumpOut.Write(")");
					}
				}
				progressWriter.Finish();
				dumpOut.WriteLine(")");
				dumpOut.WriteLine("  (rows " + rows + "))\n");

				OutputDevice.Message.Write("(" + rows + " rows)\n");
				long execTime = (long)(DateTime.Now - startTime).TotalMilliseconds;

				OutputDevice.Message.Write("dumping '" + dumpSource.TableName + "' took ");
				TimeRenderer.PrintTime(execTime, OutputDevice.Message);
				OutputDevice.Message.Write(" total; ");
				TimeRenderer.PrintFraction(execTime, rows, OutputDevice.Message);
				OutputDevice.Message.WriteLine(" / row");
				if (expectedRows >= 0 && rows != expectedRows) {
					OutputDevice.Message.WriteLine(" == Warning: 'select count(*)' in the"
										  + " beginning resulted in " + expectedRows
										  + " but the dump exported " + rows
										  + " rows == ");
				}

				if (!command.IsRunning) {
					OutputDevice.Message.WriteLine(" == INTERRUPTED. Wait for statement to cancel.. ==");
					if (stmt != null)
						stmt.Cancel();
				}
			} catch (Exception e) {
				//HenPlus.msg().println(selectStmt.toString());
				throw e; // handle later.
			} finally {
				if (rset != null) {
					try {
						rset.Close();
					} catch (Exception) {

					}
				}
			}
			return CommandResultCode.Success;
		}

		private static void quoteString(TextWriter output, String input) {
			StringBuilder buf = new StringBuilder();
			buf.Append("'");
			int len = input.Length;
			for (int i = 0; i < len; ++i) {
				char c = input[i];
				if (c == '\'' || c == '\\') {
					buf.Append("\\");
				}
				buf.Append(c);
			}
			buf.Append("'");
			output.Write(buf.ToString());
		}

		// to make the field-name and field-type nicely aligned
		private void printWidth(TextWriter output, string s, int width, bool comma) {
			if (comma)
				output.Write(", ");
			output.Write("'");
			output.Write(s);
			output.Write("'");
			for (int i = s.Length; i < width; ++i) {
				output.Write(' ');
			}
		}
	}
}