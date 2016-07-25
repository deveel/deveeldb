using System;
using System.Text;

using CommandLine;
using CommandLine.Text;

using Deveel.Data.Client;

using Mono.Terminal;

namespace deveeldb.cli {
	class Program {
		private const string Heading = "    DeveelDB Command Line Shell";
		private const string Copyright = "        (c) 2016 Deveel - This software is released under the GPLv3 License by Deveel";

		static int Main(string[] args) {
			var options = new Options();
			if (!Parser.Default.ParseArguments(args, options)) {
				PrintOptionsError(options);
				return 1;
			}

			if (options.Help) {
				PrintHelp(options);
				return 0;
			}

			Console.Out.WriteLine(Heading);
			Console.Out.WriteLine(Copyright);

			var lineEditor = new LineEditor("DeveelDB.Shell");
			lineEditor.TabAtStartCompletes = true;
			lineEditor.AutoCompleteEvent += AutoComplete;

			var builder = new StringBuilder();

			string line;
			while ((line = lineEditor.Edit("deveeldb> ", "")) != null) {
				if (String.Equals(line, Environment.NewLine)) {
				} else if (String.Equals(line, "exit", StringComparison.OrdinalIgnoreCase)) {
					break;
				} else {
					builder.Append(line);
				}
			}

			return 0;
		}

		private static void PrintHelp(Options options) {
			var helpText = new HelpText {
				AddDashesToOption = true,
				AdditionalNewLineAfterOption = true,
				MaximumDisplayWidth = Console.WindowWidth,
				Heading = Heading,
				Copyright = Copyright
			};

			helpText.AddOptions(options, "required");
			Console.Error.Write(helpText.ToString());
		}

		private static void PrintOptionsError(Options options) {
			foreach (var error in options.ParserState.Errors) {
				if (error.ViolatesRequired) {
					Console.Error.WriteLine("Option {0}|{1} is required", error.BadOption.LongName, error.BadOption.ShortName);
				} else if (error.ViolatesFormat) {
					Console.Error.WriteLine("Option {0}|{1} is has wrong format", error.BadOption.LongName, error.BadOption.ShortName);
				}
			}
		}

		private static LineEditor.Completion AutoComplete(string text, int pos) {
			return new LineEditor.Completion(text, new string[0]);
		}
	}
}
