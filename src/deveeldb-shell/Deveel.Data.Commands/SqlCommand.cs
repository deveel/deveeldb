using System;
using System.Collections;

using Deveel.Commands;
using Deveel.Data.Client;
using Deveel.Data.Shell;
using Deveel.Design;
using Deveel.Shell;

namespace Deveel.Data.Commands {
	abstract class SqlCommand : Command {
		protected SqlCommand() {
			_columnDelimiter = "|";
			rowLimit = 2000;
			showHeader = true;
			showFooter = true;
		}

		private DeveelDbCommand dbCommand;
		private String _columnDelimiter;
		private int rowLimit;
		private bool showHeader;
		private bool showFooter;
		private volatile bool running;

		private static StatementCanceller statementCanceller;
		private static LongRunningTimeDisplay longRunningDisplay;

		private static readonly string[] TableCompleterKeyword = { "FROM", "INTO", "UPDATE", "TABLE", "ALIAS", "VIEW", /*create index*/"ON" };

		public override bool RequiresContext {
			get { return true; }
		}

		protected virtual bool IsUpdateCommand {
			get { return true; }
		}

		private void SetColumnDelimiter(String value) {
			_columnDelimiter = value;
		}

		public string ColumnDelimiter {
			get { return _columnDelimiter; }
		}

		private void SetRowLimit(int value) {
			rowLimit = value;
		}

		public int RowLimit {
			get { return rowLimit; }
		}

		public void SetShowHeader(bool value) {
			showHeader = value;
		}

		private bool ShowHeader {
			get { return showHeader; }
		}

		private void SetShowFooter(bool value) {
			showFooter = value;
		}

		public bool ShowFooter {
			get { return showFooter; }
		}

		protected override void OnInit() {
			if (!Application.Properties.HasProperty("column-delimiter"))
				Application.Properties.RegisterProperty("column-delimiter", new SQLColumnDelimiterProperty(this));
			if (!Application.Properties.HasProperty("sql-result-limit"))
				Application.Properties.RegisterProperty("sql-result-limit", new RowLimitProperty(this));
			if (!Application.Properties.HasProperty("sql-result-showheader"))
				Application.Properties.RegisterProperty("sql-result-showheader", new ShowHeaderProperty(this));
			if (!Application.Properties.HasProperty("sql-result-showfooter"))
				Application.Properties.RegisterProperty("sql-result-showfooter", new ShowFooterProperty(this));

			if (statementCanceller == null) {
				statementCanceller = new StatementCanceller(new CurrentStatementCancelTarget(this));
				statementCanceller.StartThread();
			}
			if (longRunningDisplay == null) {
				longRunningDisplay = new LongRunningTimeDisplay("statement running", 30000);
				longRunningDisplay.StartThread();
			}
		}

		public override bool IsComplete(string command) {
			command = command.ToUpper(); // fixme: expensive.
			if (command.StartsWith("COMMIT") || 
				command.StartsWith("ROLLBACK"))
				return true;

			if (command.Length >= 1) {
				int lastPos = command.Length - 1;
				if (command[lastPos] == '\n' && 
					command[lastPos - 1] == '/' && 
					command[lastPos - 2] == '\n')
					return true;
			}
			return false;
		}

		public override CommandResultCode Execute(object context, string[] args) {
			SqlSession session = (SqlSession) context;

			string parmas = String.Join(" ", args);
			string commandText = Name + " " + parmas;

			if (commandText.EndsWith("/"))
				commandText = commandText.Substring(0, commandText.Length - 1);

			DateTime startTime = DateTime.Now;
			TimeSpan lapTime = TimeSpan.Zero;
			TimeSpan execTime = TimeSpan.Zero;
			DeveelDbDataReader rset = null;
			running = true;
			SignalInterruptHandler.Current.Push(statementCanceller);

			try {
				if (commandText.StartsWith("commit")) {
					session.Write("commit..");
					session.Commit();
					session.WriteLine(".done.");
				} else if (commandText.StartsWith("rollback")) {
					session.Write("rollback..");
					session.Rollback();
					session.WriteLine(".done.");
				} else {
					int updateCount = -1;

					dbCommand = session.Connection.CreateCommand();
					dbCommand.CommandText = commandText;

					statementCanceller.Arm();
					longRunningDisplay.Arm();
					if (IsUpdateCommand) {
						updateCount = this.dbCommand.ExecuteNonQuery();
					} else {
						rset = this.dbCommand.ExecuteReader();
					}
					longRunningDisplay.Disarm();

					if (!running) {
						Application.MessageDevice.WriteLine("cancelled");
						return CommandResultCode.Success;
					}

					if (rset != null) {
						ResultSetRenderer renderer = new ResultSetRenderer(rset, ColumnDelimiter, ShowHeader, ShowFooter,
						                                                   RowLimit, Application.OutputDevice);
						SignalInterruptHandler.Current.Push(renderer);
						int rows = renderer.Execute();
						SignalInterruptHandler.Current.Pop();
						if (renderer.LimitReached) {
							session.WriteLine("limit of " + RowLimit + " rows reached ..");
							session.Write("> ");
						}
						session.Write(rows + " row" + ((rows == 1) ? "" : "s") + " in result");
						lapTime = renderer.FirstRowTime - startTime;
					} else {
						if (updateCount >= 0) {
							session.Write("affected " + updateCount + " rows");
						} else {
							session.Write("ok.");
						}
					}

					execTime = DateTime.Now - startTime;
					session.Write(" (");
					if (lapTime > TimeSpan.Zero) {
						session.Write("first row: ");
						if (session.PrintMessages) {
							TimeRenderer.PrintTime((long)lapTime.TotalMilliseconds, Application.MessageDevice);
						}
						session.Write("; total: ");
					}

					if (session.PrintMessages) {
						TimeRenderer.PrintTime((long)execTime.TotalMilliseconds, Application.MessageDevice);
					}
					session.WriteLine(")");
				}

				// be smart and retrigger hashing of the tablenames.
				if (Name.Equals("drop") || Name.Equals("create")) {
					session.UnhashCompleters();
				}
			} catch (Exception e) {
				String msg = e.Message;
				if (msg != null) {
					// oracle appends a newline to the message for some reason.
					Application.MessageDevice.WriteLine("FAILURE: " + msg.Trim());
				}
				return CommandResultCode.ExecutionFailed;
			} finally {
				statementCanceller.Disarm();
				longRunningDisplay.Disarm();
				try {
					if (rset != null) 
						rset.Close();
				} catch (Exception) {
					
				}

				SignalInterruptHandler.Current.Pop();
			}

			return CommandResultCode.ExecutionFailed;
		}

		public override IEnumerator Complete(CommandDispatcher dispatcher, string partialCommand, string lastWord) {
			SqlSession session = Application.CurrentContext as SqlSession;
			if (session == null)
				return null;

			String canonCmd = partialCommand.ToUpper();

			// look for keywords that expect table names
			int tableMatch = -1;
			for (int i = 0; i < TableCompleterKeyword.Length; ++i) {
				int match = canonCmd.IndexOf(TableCompleterKeyword[i]);
				if (match >= 0) {
					tableMatch = match + TableCompleterKeyword[i].Length;
					break;
				}
			}

			if (tableMatch < 0) {
				// try to complete all columns from all tables since
				// we don't know yet what table the column will be from.
				return session.CompleteAllColumns(lastWord);
			}

			int endTabMatch = -1;  // where the table declaration ends.
			if (canonCmd.IndexOf("UPDATE") >= 0) {
				endTabMatch = canonCmd.IndexOf("SET");
			} else if (canonCmd.IndexOf("INSERT") >= 0) {
				endTabMatch = canonCmd.IndexOf("(");
			} else if (canonCmd.IndexOf("WHERE") >= 0) {
				endTabMatch = canonCmd.IndexOf("WHERE");
			} else if (canonCmd.IndexOf("ORDER BY") >= 0) {
				endTabMatch = canonCmd.IndexOf("ORDER BY");
			} else if (canonCmd.IndexOf("GROUP BY") >= 0) {
				endTabMatch = canonCmd.IndexOf("GROUP BY");
			}
			if (endTabMatch < 0) {
				endTabMatch = canonCmd.IndexOf(";");
			}

			if (endTabMatch > tableMatch) {
				// column completion for the tables mentioned between in the
				// table area. This acknowledges as well aliases and prepends
				// the names with these aliases, if necessary.
				string tables = partialCommand.Substring(tableMatch, endTabMatch - tableMatch).Trim();

				Hashtable tmp = new Hashtable();
				IEnumerator it = TableDeclParser(tables).GetEnumerator();
				while (it.MoveNext()) {
					DictionaryEntry entry = (DictionaryEntry)it.Current;
					string alias = (String)entry.Key;
					string tabName = (String)entry.Value;
					tabName = session.CorrectTableName(tabName);
					if (tabName == null)
						continue;

					ICollection columns = session.ColumnsFor(tabName);
					IEnumerator cit = columns.GetEnumerator();
					while (cit.MoveNext()) {
						string col = (string)cit.Current;
						IList aliases = (IList)tmp[col];
						if (aliases == null) 
							aliases = new ArrayList();
						aliases.Add(alias);
						tmp.Add(col, aliases);
					}
				}

				NameCompleter completer = new NameCompleter();
				it = tmp.GetEnumerator();
				while (it.MoveNext()) {
					DictionaryEntry entry = (DictionaryEntry)it.Current;
					string col = (string)entry.Key;
					IList aliases = (IList)entry.Value;
					if (aliases.Count == 1) {
						completer.AddName(col);
					} else {
						IEnumerator ait = aliases.GetEnumerator();
						while (ait.MoveNext()) {
							completer.AddName(ait.Current + "." + col);
						}
					}
				}
				return completer.GetAlternatives(lastWord);
			} else { // table completion.
				return session.CompleteTableName(lastWord);
			}
		}

		#region CurrentStatementCancelTarget

		/// <summary>
		/// A statement cancel target that accesses the instance 
		/// wide statement.
		/// </summary>
		private class CurrentStatementCancelTarget : StatementCanceller.CancelTarget {
			public CurrentStatementCancelTarget(SqlCommand command) {
				this.command = command;
			}

			private readonly SqlCommand command;

			public void CancelRunningStatement() {
				try {
					command.Application.MessageDevice.WriteLine("cancel statement...");
					command.Application.MessageDevice.Flush();

					CancelWriter info = new CancelWriter(command.Application.MessageDevice);
					info.Write("please wait");
					command.dbCommand.Cancel();
					info.Cancel();

					command.Application.MessageDevice.WriteLine("done.");
					command.running = false;
				} catch (Exception) {
					
				}
			}
		}

		#endregion

		// parses 'tablename ((AS)? alias)? [,...]' and returns a map, that maps
		// the names (or aliases) to the tablenames.
		private static IDictionary TableDeclParser(String tableDecl) {
			string[] tokenizer = tableDecl.Split('\t', '\n', '\r', '\f');
			Hashtable result = new Hashtable();
			String tok;
			String table = null;
			String alias = null;
			int state = 0;
			for (int i = 0; i < tokenizer.Length; i++) {
				tok = tokenizer[i];
				if (tok.Length == 1 && Char.IsWhiteSpace(tok[0]))
					continue;

				switch (state) {
					case 0: { // initial/endstate
							table = tok;
							alias = tok;
							state = 1;
							break;
						}
					case 1: { // table seen, waiting for potential alias.
							if ("AS".Equals(tok.ToUpper()))
								state = 2;
							else if (",".Equals(tok)) {
								state = 0; // we are done.
							} else {
								alias = tok;
								state = 3;
							}
							break;
						}
					case 2: { // 'AS' seen, waiting definitly for alias.
							if (",".Equals(tok)) {
								// error: alias missing for $table.
								state = 0;
							} else {
								alias = tok;
								state = 3;
							}
							break;
						}
					case 3: {  // waiting for ',' at end of 'table (as)? alias'
							if (!",".Equals(tok))
								// error: ',' expected.
							state = 0;
							break;
						}
				}

				if (state == 0) {
					if (alias != null)
						result.Add(alias, table);
				}
			}

			// store any unfinished state..
			if (state == 1 || state == 3) {
				if (alias != null)
					result.Add(alias, table);
			} else if (state == 2) {
				// error: alias expected for $table.
			}

			return result;
		}

		#region RowLimitProperty

		private class RowLimitProperty : PropertyHolder {
			public RowLimitProperty(SqlCommand command)
				: base(command.RowLimit.ToString()) {
				this.command = command;
			}

			private readonly SqlCommand command;

			protected override String OnValueChanged(String newValue) {
				newValue = newValue.Trim();
				int newIntValue;
				try {
					newIntValue = Int32.Parse(newValue);
				} catch (FormatException e) {
					throw new ArgumentException("cannot parse '" + newValue + "' as integer");
				}
				if (newIntValue < 1) {
					throw new ArgumentException("value cannot be less than 1");
				}
				command.SetRowLimit(newIntValue);
				return newValue;
			}

			public override string DefaultValue {
				get { return "2000"; }
			}

			public override String ShortDescription {
				get { return "set the maximum number of rows printed"; }
			}
		}

		#endregion

		#region SQLColumnDelimiterProperty

		private class SQLColumnDelimiterProperty : PropertyHolder {
			public SQLColumnDelimiterProperty(SqlCommand command)
				: base(command.ColumnDelimiter) {
				this.command = command;
			}

			private readonly SqlCommand command;

			protected override String OnValueChanged(String newValue) {
				command.SetColumnDelimiter(newValue);
				return newValue;
			}

			public override String ShortDescription {
				get { return "modify column separator in query results"; }
			}

			public override String DefaultValue {
				get { return "|"; }
			}

			public override String LongDescription {
				get {
					return "Set another string that is used to separate columns in\n" +
					       "SQL result sets. Usually this is a pipe-symbol '|', but\n" +
					       "maybe you want to have an empty string ?";
				}
			}
		}

		#endregion

		#region ShowHeaderProperty

		private class ShowHeaderProperty : BooleanPropertyHolder {

			public ShowHeaderProperty(SqlCommand command)
				: base(true) {
				this.command = command;
			}

			private readonly SqlCommand command;

			public override void OnBooleanPropertyChanged(bool value) {
				command.SetShowHeader(value);
			}

			public override String DefaultValue {
				get { return "on"; }
			}

			public override String ShortDescription {
				get { return "switches if header in selected tables should be shown"; }
			}
		}

		#endregion

		#region ShowFooterProperty

		private class ShowFooterProperty : BooleanPropertyHolder {

			public ShowFooterProperty(SqlCommand command)
				: base(true) {
				this.command = command;
			}

			private readonly SqlCommand command;

			public override void OnBooleanPropertyChanged(bool value) {
				command.SetShowFooter(value);
			}

			public override String DefaultValue {
				get { return "on"; }
			}

			public override String ShortDescription {
				get { return "switches if footer in selected tables should be shown"; }
			}
		}

		#endregion
	}
}