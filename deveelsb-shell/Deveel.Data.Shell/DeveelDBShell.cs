using System;
using System.Collections;
using System.IO;
using System.Text;

using Deveel.Commands;
using Deveel.Configuration;
using Deveel.Shell;

namespace Deveel.Data.Shell {
	public sealed class DeveelDBShell : ShellApplication, IInterruptable {
		private DeveelDBShell(string[] args)
			: base(args) {
		}

		private SqlStatementSeparator commandSeparator;
		private ClientSessionManager sessionManager;

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

		public ClientSessionManager SessionManager {
			get { return sessionManager; }
		}

		public ClientSession CurrentSession {
			get { return SessionManager.CurrentSession; }
		}

		protected override void Init(CommandLine args) {
			commandSeparator = new SqlStatementSeparator();
			sessionManager = ClientSessionManager.Current;

			Properties.RegisterProperty("comments-remove", commandSeparator.GetRemoveCommentsProperty());
		}


		private string VarSubstitute(string input, IDictionary variables) {
			int pos = 0;
			int endpos = 0;
			int startVar = 0;
			StringBuilder result = new StringBuilder();
			string varname;
			bool hasBrace = false;
			bool knownVar = false;

			if (input == null)
				return null;

			if (variables == null)
				return input;

			while ((pos = input.IndexOf('$', pos)) >= 0) {
				startVar = pos;
				if (input[pos + 1] == '$') {
					// quoting '$'
					result.Append(input.Substring(endpos, pos - endpos));

					endpos = pos + 1;
					pos += 2;
					continue;
				}

				hasBrace = (input[pos + 1] == '{');

				// text between last variable and here
				result.Append(input.Substring(endpos, pos - endpos));

				if (hasBrace)
					pos++;

				endpos = pos + 1;
				while (endpos < input.Length &&
					Char.IsLetterOrDigit(input[endpos]))
					endpos++;

				varname = input.Substring(pos + 1, endpos - (pos + 1));

				if (hasBrace) {
					while (endpos < input.Length && input[endpos] != '}')
						++endpos;

					++endpos;
				}
				if (endpos > input.Length) {
					if (variables.Contains(varname))
						Console.Error.WriteLine("warning: missing '}' for variable '" + varname + "'.");

					result.Append(input.Substring(startVar));
					break;
				}

				if (variables.Contains(varname)) {
					result.Append(variables[varname]);
				} else {
					Console.Error.WriteLine("warning: variable '" + varname + "' not set.");
					result.Append(input.Substring(startVar, endpos - startVar));
				}

				pos = endpos;
			}

			if (endpos < input.Length)
				result.Append(input.Substring(endpos));

			return result.ToString();
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

		protected override LineExecutionResultCode ExecuteLine(string line) {
			LineExecutionResultCode resultCode = LineExecutionResultCode.Empty;

			int startWhite = 0;

			// Oracle-like comment REM...
			if (line.Length >= (3 + startWhite) &&
				(line.Substring(startWhite, 3).ToUpper().Equals("REM")) &&
				(line.Length == 3 || Char.IsWhiteSpace(line[3]))) {
				return LineExecutionResultCode.Empty;
			}

			StringBuilder lineBuilder = new StringBuilder(line);
			lineBuilder.Append(Environment.NewLine);
			commandSeparator.Append(lineBuilder.ToString());

			resultCode = LineExecutionResultCode.Incomplete;

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