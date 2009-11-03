using System;
using System.Collections;
using System.Threading;

using Deveel.Commands;
using Deveel.Data.Client;
using Deveel.Data.Shell;
using Deveel.Data.Util;
using Deveel.Design;
using Deveel.Shell;

namespace Deveel.Data.Commands {
	abstract class SqlCommand : Command {
		protected SqlCommand() {
			_columnDelimiter = "|";
			_rowLimit = 2000;
			_showHeader = true;
			_showFooter = true;
		}

		private DeveelDbCommand _stmt;
		private String _columnDelimiter;
		private int _rowLimit;
		private bool _showHeader;
		private bool _showFooter;
		private volatile bool _running;
		private StatementCanceller _statementCanceller;
		private LongRunningTimeDisplay _longRunningDisplay;

		private static readonly String[] TABLE_COMPLETER_KEYWORD = { "FROM", "INTO", "UPDATE", "TABLE", "ALIAS", "VIEW", /*create index*/"ON" };

		public override bool RequiresContext {
			get { return true; }
		}

		protected abstract bool IsUpdateCommand { get; }

		private void SetColumnDelimiter(String value) {
			_columnDelimiter = value;
		}

		public string ColumnDelimiter {
			get { return _columnDelimiter; }
		}

		private void SetRowLimit(int rowLimit) {
			_rowLimit = rowLimit;
		}

		public int RowLimit {
			get { return _rowLimit; }
		}

		public void SetShowHeader(bool b) {
			_showHeader = b;
		}

		private bool ShowHeader {
			get { return _showHeader; }
		}

		private void SetShowFooter(bool b) {
			_showFooter = b;
		}

		public bool ShowFooter {
			get { return _showFooter; }
		}

		protected override void OnInit() {
			Application.Properties.RegisterProperty("column-delimiter", new SQLColumnDelimiterProperty(this));
			Application.Properties.RegisterProperty("sql-result-limit", new RowLimitProperty(this));
			Application.Properties.RegisterProperty("sql-result-showheader", new ShowHeaderProperty(this));
			Application.Properties.RegisterProperty("sql-result-showfooter", new ShowFooterProperty(this));
			_statementCanceller = new StatementCanceller(new CurrentStatementCancelTarget(this));
			new Thread(_statementCanceller.run).Start();
			_longRunningDisplay = new LongRunningTimeDisplay("statement running", 30000);
			new Thread(_longRunningDisplay.run).Start();
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
			string command = Name + " " + parmas;

			if (command.EndsWith("/"))
				command = command.Substring(0, command.Length - 1);

			DateTime startTime = DateTime.Now;
			TimeSpan lapTime = TimeSpan.Zero;
			TimeSpan execTime = TimeSpan.Zero;
			DeveelDbDataReader rset = null;
			_running = true;
			SignalInterruptHandler.Current.Push(_statementCanceller);

			try {
				if (command.StartsWith("commit")) {
					session.Write("commit..");
					session.Commit();
					session.WriteLine(".done.");
				} else if (command.StartsWith("rollback")) {
					session.Write("rollback..");
					session.Rollback();
					session.WriteLine(".done.");
				} else {
					int updateCount = -1;

					_stmt = session.Connection.CreateCommand();
					_statementCanceller.arm();
					_longRunningDisplay.arm();
					if (IsUpdateCommand) {
						updateCount = _stmt.ExecuteNonQuery();
					} else {
						rset = _stmt.ExecuteReader();
					}
					_longRunningDisplay.disarm();

					if (!_running) {
						Application.MessageDevice.WriteLine("cancelled");
						return CommandResultCode.Success;
					}

					if (rset != null) {
						ResultSetRenderer renderer = new ResultSetRenderer(rset, ColumnDelimiter, ShowHeader, ShowFooter,
						                                                   RowLimit, Application.OutputDevice);
						SignalInterruptHandler.Current.Push(renderer);
						int rows = renderer.execute();
						SignalInterruptHandler.Current.Pop();
						if (renderer.limitReached()) {
							session.WriteLine("limit of " + RowLimit + " rows reached ..");
							session.Write("> ");
						}
						session.Write(rows + " row" + ((rows == 1) ? "" : "s") + " in result");
						lapTime = renderer.getFirstRowTime() - startTime;
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
					//TODO: tableCompleter.unhash(session);
				}
			} catch (Exception e) {
				String msg = e.Message;
				if (msg != null) {
					// oracle appends a newline to the message for some reason.
					Application.MessageDevice.WriteLine("FAILURE: " + msg.Trim());
				}
				return CommandResultCode.ExecutionFailed;
			} finally {
				_statementCanceller.disarm();
				_longRunningDisplay.disarm();
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
			/*
			 * look for keywords that expect table names
			 */
			int tableMatch = -1;
			for (int i = 0; i < TABLE_COMPLETER_KEYWORD.Length; ++i) {
				int match = canonCmd.IndexOf(TABLE_COMPLETER_KEYWORD[i]);
				if (match >= 0) {
					tableMatch = match + TABLE_COMPLETER_KEYWORD[i].Length;
					break;
				}
			}

			if (tableMatch < 0) {
				/*
				 * ok, try to complete all columns from all tables since
				 * we don't know yet what table the column will be from.
				 */
				return session.completeAllColumns(lastWord);
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
				/*
				 * column completion for the tables mentioned between in the
				 * table area. This acknowledges as well aliases and prepends
				 * the names with these aliases, if necessary.
				 */
				String tables = partialCommand.Substring(tableMatch, endTabMatch - tableMatch);
				Hashtable tmp = new Hashtable();
				IEnumerator it = tableDeclParser(tables).GetEnumerator();
				while (it.MoveNext()) {
					DictionaryEntry entry = (DictionaryEntry)it.Current;
					String alias = (String)entry.Key;
					String tabName = (String)entry.Value;
					tabName = session.correctTableName(tabName);
					if (tabName == null)
						continue;
					ICollection columns = session.columnsFor(tabName);
					IEnumerator cit = columns.GetEnumerator();
					while (cit.MoveNext()) {
						String col = (String)cit.Current;
						IList aliases = (IList)tmp[col];
						if (aliases == null) aliases = new ArrayList();
						aliases.Add(alias);
						tmp.Add(col, aliases);
					}
				}
				NameCompleter completer = new NameCompleter();
				it = tmp.GetEnumerator();
				while (it.MoveNext()) {
					DictionaryEntry entry = (DictionaryEntry)it.Current;
					String col = (String)entry.Key;
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
				return session.completeTableName(lastWord);
			}
		}

		/** 
 * A statement cancel target that accesses the instance
 * wide statement.
 */
		private class CurrentStatementCancelTarget : StatementCanceller.CancelTarget {
			public CurrentStatementCancelTarget(SqlCommand command) {
				this.command = command;
			}

			private readonly SqlCommand command;

			public void cancelRunningStatement() {
				try {
					command.Application.MessageDevice.WriteLine("cancel statement...");
					command.Application.MessageDevice.Flush();
					CancelWriter info = new CancelWriter(command.Application.MessageDevice);
					info.print("please wait");
					command._stmt.Cancel();
					info.cancel();
					command.Application.MessageDevice.WriteLine("done.");
					command._running = false;
				} catch (Exception e) {
					
				}
			}
		}

		/**
     * parses 'tablename ((AS)? alias)? [,...]' and returns a map, that maps
     * the names (or aliases) to the tablenames.
     */
		private IDictionary tableDeclParser(String tableDecl) {
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
							if (!",".Equals(tok)) {
								// error: ',' expected.
							}
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
					String dsc;
					dsc = "\tSet another string that is used to separate columns in\n"
						  + "\tSQL result sets. Usually this is a pipe-symbol '|', but\n"
						  + "\tmaybe you want to have an empty string ?";
					return dsc;
				}
			}
		}

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

			/**
			 * return a short descriptive string.
			 */
			public override String ShortDescription {
				get { return "switches if header in selected tables should be shown"; }
			}
		}

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

			/**
			 * return a short descriptive string.
			 */
			public override String ShortDescription {
				get { return "switches if footer in selected tables should be shown"; }
			}
		}
	}
}