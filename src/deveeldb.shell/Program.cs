using System;
using System.Text;

using CommandLine;
using CommandLine.Text;

using Deveel.Data.Client;
using Deveel.Data.Client.Commands;

using DryIoc;

using Mono.Terminal;

namespace deveeldb.cli {
	class Program {
		private const string Heading = "    DeveelDB Command Line Shell";
		private const string Copyright = "        (c) 2016 Deveel - This software is released under the GPLv3 License by Deveel";

		static int Main(string[] args) {
			SetOutput();

			var options = new Options();
			if (!Parser.Default.ParseArguments(args, options)) {
				PrintOptionsError(options);
				return 1;
			}

			if (options.Help) {
				PrintHelp(options);
				return 0;
			}

			Console.CancelKeyPress += (sender, eventArgs) => {
				if (eventArgs.Cancel &&
				    eventArgs.SpecialKey == ConsoleSpecialKey.ControlC)
					InterruptHandler.Default.Signal();
			};

			Console.Out.WriteLine(Heading);
			Console.Out.WriteLine(Copyright);

			RegisterServices();

			HandleOptions(options);

			var app = new ClientApplication(CommandRegistry.Current);
			app.Execute();

			return 0;
		}

		private static void SetOutput() {
			Output.Current = new ConsoleOutputTarget();
			Output.Error = new ConsoleOutputTarget();
		}

		private static void RegisterServices() {
			var container = new Container();

			RegisterCommands(container);
		}

		private static void RegisterCommands(Container container) {
			var registry = new DryIocCommandRegistry(container);

			// TODO:

			CommandRegistry.Current = registry;
		}

		private static void HandleOptions(IOptions options) {
			var commandNames = CommandRegistry.Current.CommandNames;
			foreach (var commandName in commandNames) {
				var command = CommandRegistry.Current.ResolveCommand(commandName);

				try {
					if (command.IsOptionsHandler())
						command.HandleOptions(options);
				} catch (Exception ex) {
					Output.Error.WriteLine("Command '{0}' could not handle the options because of an unhandled error.", command.CommandInfo.Name);
					Output.Error.Write(ex.Message);
				}
			}
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
			Output.Error.Write(helpText.ToString());
		}

		private static void PrintOptionsError(Options options) {
			foreach (var error in options.ParserState.Errors) {
				if (error.ViolatesRequired) {
					Output.Error.WriteLine("Option {0}|{1} is required", error.BadOption.LongName, error.BadOption.ShortName);
				} else if (error.ViolatesFormat) {
					Output.Error.WriteLine("Option {0}|{1} is has wrong format", error.BadOption.LongName, error.BadOption.ShortName);
				}
			}
		}
	}
}
