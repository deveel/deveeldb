using System;
using System.IO;
using System.Text;

using Deveel.Commands;
using Deveel.Configuration;
using Deveel.Shell;

namespace Deveel.Data.Shell {
	[Option("u", "user", true, "the name of the user operating")]
	[Option("p", "pass", true, "the password used to identify the user")]
	[Option("d", "database", true, "the name of a database")]
	[Option("C", true, "an configuration optional parameter", ValueSeparator = '=', ArgumentName = "key=value")]
	[Option("path", true, "the base context path of the database system")]
	[Option("?", "help", false, "prints the help usage")]
	public sealed class DeveelDBShell : ShellApplication, IInterruptable {
		private DeveelDBShell(string[] args)
			: base(args) {
		}

		private SqlStatementSeparator commandSeparator;
		private SessionManager sessionManager;
		private Connections connections;

		protected override string Prompt {
			get { return "DeveelDB> "; }
		}

		protected override string About {
			get {
				StringWriter writer = new StringWriter();
				writer.WriteLine("---------------------------------------------------------------------------");
				writer.WriteLine(" {ApplicationName} {Version} {Copyright}");
				writer.WriteLine();
				writer.WriteLine(" Deveel DB Shell is provided AS IS and comes with ABSOLUTELY NO WARRANTY");
				writer.WriteLine(" This is free software, and you are welcome to redistribute it under the");
				writer.WriteLine(" conditions of the {License}");
				writer.WriteLine("---------------------------------------------------------------------------");
				return writer.ToString();
			}
		}

		protected override string License {
			get { return "GNU Public License <http://www.gnu.org/licenses/gpl.txt>"; }
		}

		protected override string ConfigDirectoryName {
			get { return ".deveeldb"; }
		}

		public SessionManager SessionManager {
			get { return sessionManager; }
		}

		public SqlSession CurrentSession {
			get { return SessionManager.CurrentSession; }
		}

		public override object CurrentContext {
			get { return CurrentSession; }
		}

		public Connections Connections {
			get {
				if (connections == null)
					connections = new Connections(this, CreateConfigurationFile("connections"));
				return connections;
			}
		}

		protected override void Init(CommandLine args) {
			commandSeparator = new SqlStatementSeparator();
			sessionManager = new SessionManager();

			Properties.RegisterProperty("comments-remove", commandSeparator.GetRemoveCommentsProperty());
		}

		protected override void OnInterrupted() {
			commandSeparator.Discard();
		}

		protected override bool OnTerminated() {
			if (CurrentContext != null) {
				Dispatcher.Execute(CurrentContext, "disconnect");
				return true;
			}
			return false;
		}

		protected override void OnConfigure(CommandLineOptions options) {
			CommandLineOptionGroup group = new CommandLineOptionGroup();

			CommandLineOption option = new CommandLineOption("n", "create", false, "creates a new database");
			group.AddOption(option);

			option = new CommandLineOption("s", "shutdown", false, "shutdowns a database.");
			group.AddOption(option);

			option = new CommandLineOption("b", "startup", false, "boots a database");
			group.AddOption(option);

			options.AddOptionGroup(group);
		}

		protected override void OnShutdown() {
			connections.Save();
		}

		public SqlSession CreateSession(string connectionString, string alias) {
			SqlSession session = new SqlSession(this, connectionString);
			sessionManager.AddSession(session, alias);
			sessionManager.SetCurrentSession(session);
			SetPrompt(session.Name + ">");
			return session;
		}

		protected override LineExecutionResultCode ExecuteLine(string line) {
			const int startWhite = 0;

			// Oracle-like comment REM...
			if (line.Length >= (3 + startWhite) &&
				(line.Substring(startWhite, 3).ToUpper().Equals("REM")) &&
				(line.Length == 3 || Char.IsWhiteSpace(line[3]))) {
				return LineExecutionResultCode.Empty;
			}

			StringBuilder lineBuilder = new StringBuilder(line);
			lineBuilder.Append(Environment.NewLine);
			commandSeparator.Append(lineBuilder.ToString());

			LineExecutionResultCode resultCode = LineExecutionResultCode.Incomplete;

			while (commandSeparator.MoveNext()) {
				string completeCommand = commandSeparator.Current;
				completeCommand = Settings.Substitute(completeCommand);

				Command c = Dispatcher.GetCommand(completeCommand);

				if (c == null) {
					commandSeparator.Consumed();
					// do not shadow successful executions with the 'line-empty'
					// message. Background is: when we consumed a command, that
					// is complete with a trailing ';', then the following newline
					// would be considered as empty command. So return only the
					// 'Empty', if we haven't got a succesfully executed line.
					if (resultCode != LineExecutionResultCode.Executed)
						resultCode = LineExecutionResultCode.Empty;
				} else if (!c.IsComplete(completeCommand)) {
					commandSeparator.Cont();
					resultCode = LineExecutionResultCode.Incomplete;
				} else {
					// Console.Error.WriteLine("SUBST: " + completeCommand);

					Execute(CurrentContext, completeCommand.Trim());

					commandSeparator.Consumed();

					resultCode = LineExecutionResultCode.Executed;
				}
			}

			return resultCode;
		}

		public void Interrupt() {
			MessageDevice.AttributeBold();
			MessageDevice.Write(" ..discard current line; press [RETURN]");
			MessageDevice.AttributeReset();

			SetInterrupted();
		}

		public void PushBuffer() {
			commandSeparator.Push();
		}

		public void PopBuffer() {
			commandSeparator.Pop();
		}

		[STAThread]
		static void Main(string[] args) {
			try {
				Run(typeof (DeveelDBShell), args);
				Environment.Exit(0);
			} catch (Exception) {
				Environment.Exit(1);
			}
		}
	}
}