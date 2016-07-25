using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using CommandLine;
using CommandLine.Text;

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
					Execute(builder.ToString());
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

		private static void Execute(string commandText) {
			// TODO: execute and print the results
		}

		private static LineEditor.Completion AutoComplete(string text, int pos) {
			string[] result;
			if (String.IsNullOrEmpty(text)) {
				result = commandTexts;
			} else {
				var input = text.Substring(0, pos);

				bool allUpper = input.All(Char.IsUpper);
				bool allLower = input.All(Char.IsLower);

				result = commandTexts.Where(x => x.StartsWith(input, StringComparison.OrdinalIgnoreCase))
					.Select(x => x.Substring(input.Length))
					.Select(x => {
						if (allUpper)
							return x.ToUpperInvariant();
						if (allLower)
							return x.ToLowerInvariant();
						return x;
					})
					.ToArray();
			}

			return new LineEditor.Completion(text, result);
		}

		private static readonly string[] commandTexts = new[] {
			"CREATE",
			"DELETE",
			"INSERT",
			"SELECT"
		};

		#region Options

		class Options {
			[Option('c', "connection-type", DefaultValue = "Deveel.Data.Client.DeveelDbConnection, deveel.client", HelpText = "The type of the connection to use for executing the remote commands")]
			public string ConnectionType { get; set; }

			[Option('m', "embedded", DefaultValue = false, HelpText = "IF set to true uses the embedded connection")]
			public bool UseEmbedded { get; set; }

			[Option('u', "user", HelpText = "The name of the user trying to connect")]
			public string UserName { get; set; }

			[Option('p', "password", HelpText = "The password for authenticating the user")]
			public string Password { get; set; }

			[Option('x', "connection-string", HelpText = "The full connection string to the database server")]
			public string ConnectionString { get; set; }

			[Option('r', "host", HelpText = "The host address of the server")]
			public string Host { get; set; }

			[Option('o', "port", HelpText = "The port of the server to connect")]
			public int? Port { get; set; }

			[Option('h', "help", HelpText = "Displays the help text")]
			public bool Help { get; set; }

			[ParserState]
			public IParserState ParserState { get; set; }
		}

		#endregion

	}
}
