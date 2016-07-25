using System;
using System.Diagnostics;

namespace Deveel.Data.Client.Commands {
	public abstract class CommandBase : ICommand, ICompleter {
		private CommandInfo commandInfo;

		protected CommandBase(string name) {
			CommandName = name;
		}

		CommandInfo ICommand.CommandInfo {
			get {
				if (commandInfo == null)
					commandInfo = GetCommandInfo();

				return commandInfo;
			}
		}

		protected string CommandName { get; private set; }

		public virtual bool Matches(string[] tokens) {
			if (tokens == null ||
			    tokens.Length == 0)
				return false;

			return String.Equals(tokens[0], CommandName);
		}

		bool ICompleter.CanComplete(CompleteRequest request) {
			if (!CanComplete(request.Tokens, request.CurrentToken))
				return false;

			return true;
		}

		CompleteResult ICompleter.Complete(CompleteRequest request) {
			string[] suggestions = null;
			string[] errors = null;

			try {
				suggestions = Complete(request.Tokens, request.CurrentToken);
			} catch (CompletionException ex) {
				errors = ex.Errors;
			} catch (Exception ex) {
				errors = new[] {ex.Message};
			}

			if (errors != null)
				return CompleteResult.Fail(errors);

			return CompleteResult.Suggest(suggestions);
		}

		protected virtual bool CanComplete(string[] tokens, string currentToken) {
			return false;
		}

		protected virtual string[] Complete(string[] tokens, string currentToken) {
			return new string[0];
		}

		protected virtual string CommandDescription {
			get { return null; }
		}

		protected virtual CommandInfo GetCommandInfo() {
			return new CommandInfo(CommandName) {
				Description = CommandDescription
			};
		}

		CommandExecutionResult ICommand.Execute(string commandText) {
			var timer = new Stopwatch();
			timer.Start();

			try {
				var result = ExecuteCommand(commandText);

				timer.Stop();

				return new CommandExecutionResult(result, true) {
					ExecutionTime = (int) timer.ElapsedMilliseconds
				};
			} catch (Exception ex) {
				timer.Stop();

				return new CommandExecutionResult(null, false) {
					Error = ex,
					ExecutionTime = (int) timer.ElapsedMilliseconds
				};
			}
		}

		protected abstract object ExecuteCommand(string commandText);
	}
}
