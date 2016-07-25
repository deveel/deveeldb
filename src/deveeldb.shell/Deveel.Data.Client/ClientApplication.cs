using System;
using System.IO;
using System.Text;

using Deveel.Data.Client.Commands;
using Deveel.Data.Sql.Compile;

using Mono.Terminal;

namespace Deveel.Data.Client {
	public class ClientApplication : IApplication {
		private LineEditor lineEditor;

		private bool terminated;
		private bool interrupted;

		private string prompt;
		private string emptyPrompt;

		private StringBuilder historyLine;

		private SqlCommandLexer lexer;

		public ClientApplication(ICommandRegistry commandRegistry) {
			CommandRegistry = commandRegistry;

			historyLine = new StringBuilder();
			lexer = new SqlCommandLexer();

			lineEditor = new LineEditor("DeveelDB.Shell");
			lineEditor.AutoCompleteEvent += AutoComplete;
		}

		public bool Verbose { get; set; }

		private LineEditor.Completion AutoComplete(string text, int pos) {
			return new LineEditor.Completion(text, new string[0]);
		}

		public void Interrupt() {

		}

		public ICommandRegistry CommandRegistry { get; private set; }

		public void SetPrompt(string text) {
			prompt = text;

			var chars = new char[text.Length];
			for (int i = 0; i < chars.Length; i++) {
				chars[i] = ' ';
			}
			emptyPrompt = new string(chars);
		}

		public LineStatus ExecuteLine(string line) {
			throw new NotImplementedException();
		}

		public void Execute() {
			String cmdLine = null;
			String displayPrompt = prompt;
			while (!terminated) {
				interrupted = false;

				InterruptHandler.Default.Push(this);

				try {
					// TODO: support reading from file ...
					cmdLine = lineEditor.Edit(displayPrompt, "");
				} catch (EndOfStreamException) {
					// EOF on CTRL-D
					// TODO: disconnect any open client and reset the prompt...

					break; // last session closed -> exit.
				} catch (Exception e) {
					if (Verbose) {
						Output.Error.WriteLine(e.Message);
						Output.Error.WriteLine(e.StackTrace);
					}
				}

				InterruptHandler.Default.Reset();

				// anyone pressed CTRL-C
				if (interrupted) {
					historyLine.Length = 0;
					lexer.Discard();
					displayPrompt = prompt;
					continue;
				}

				if (cmdLine == null) {
					continue;
				}

				if (historyLine.Length > 0 && !cmdLine.Trim().Equals(";")) {
					historyLine.Append("\n");
				}

				historyLine.Append(cmdLine);

				var lineExecState = ExecuteLine(cmdLine);
				if (lineExecState == LineStatus.Incomplete) {
					displayPrompt = emptyPrompt;
				} else {
					displayPrompt = prompt;
				}
				if (lineExecState != LineStatus.Incomplete) {
					lineEditor.SaveHistory();
				}
			}

			InterruptHandler.Default.Reset();
		}
	}
}
