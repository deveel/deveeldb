using System;

using CommandLine;

namespace Deveel.Data.Client {
	class Options : IOptions {
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

		public bool HasOption(string option) {
			switch (option.ToLowerInvariant()) {
				default:
					return false;
			}
		}

		public object GetValue(string option) {
			throw new NotImplementedException();
		}
	}
}