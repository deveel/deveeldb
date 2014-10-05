//
//  DeveelDB TCP/IP Network Server.
//
//  Copyright (c) 2009-2014 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.
//

using System;

using Deveel.Configuration;

using Topshelf;

namespace Deveel.Data.Net {
	public sealed class TcpServer {
		private static Options GetOptions() {
			Options options = new Options();
			Option option;

			option = new Option("?", "help", false, "Prints this help.");
			options.AddOption(option);

			option = new Option("c", "conf", true,
			                               "The database configuration file to use.  If not specified then it searches " +
			                               "for 'db.conf' in the current directory.");
			option.ArgumentName = "config-file";
			options.AddOption(option);

			option = new Option("dbpath", true, "Specifies where the database data files are located.");
			option.ArgumentName = "path";
			options.AddOption(option);

			option = new Option("l", "logpath", true, "Specifies where the logs are to be kept.");
			option.ArgumentName = "logpath";
			options.AddOption(option);

			option = new Option("h", "address", true,
			                               "For multi-homed machines, allows for the database to bind to a particular " +
			                               "host address.");
			option.ArgumentName = "host";
			options.AddOption(option);

			option = new Option("u", "user", true, "The name of the user that computes the operation.");
			option.ArgumentName = "name";
			options.AddOption(option);

			option = new Option("p", "password", true,
			                               "The password used to identify the user. When not " +
			                               "provided explicitily, the application will ask to provide it in a masked way.");
			option.HasOptionalArgument = true;
			options.AddOption(option);

			option = new Option("port", true, "Sets the TCP port where the clients must connect to.");
			option.ArgumentName = "port";
			option.Type = OptionType.Number;
			options.AddOption(option);

			option = new Option("d", "database", true, "The name of the database to boot or create.");
			option.ArgumentName = "name";
			options.AddOption(option);

			option = new Option("C", true,
			                               "Where <key> is a configuration property and <value> is a value to set the " +
			                               "property to. This can be used to override any property in the configuration " +
			                               "file.  Example: -Cmaximum_worker_threads=2");
			option.ArgumentCount = 2;
			option.ArgumentName = "key=value";
			option.ValueSeparator = '=';
			options.AddOption(option);

			OptionGroup commandsGroup = new OptionGroup();

			option = new Option("n", "create", false,
			                               "Creates an empty database and adds a user with the given username and " +
			                               "password with complete privs. This will not start the database server.");
			commandsGroup.AddOption(option);

			option = new Option("s", "shutdown", false,
			                               "Shuts down the database server running on the host/port. [host] and " +
			                               "[port] are optional, they default to 'localhost' and port 9157.");
			commandsGroup.AddOption(option);

			option = new Option("b", "boot", false,
			                               "Boots the database server from the information given in the configuration " +
			                               "file. This switch is implied if no other function switch is provided.");
			commandsGroup.AddOption(option);

			option = new Option("i", "install", false,
			                               "Installs the TCP/IP service in the current machine applying the given arguments " +
			                               "to the installation process. All the configurations concerning the system " +
			                               "will be recovered from a default configuration file in the same directory of " +
			                               "the executable.");
			commandsGroup.AddOption(option);

			option = new Option("r", "uninstall", false,
			                               "If the TCP/IP service was previously installed in the current machine, this " +
			                               "command uninstalls it definitively.");
			commandsGroup.AddOption(option);

			options.AddOptionGroup(commandsGroup);

			return options;
		}
		static void Main(string[] args) {
			var parser = new PosixParser();
			var options = parser.ParseObject<ServerOptions>(args);

			var service = HostFactory.New(x => {
				x.RunAsNetworkService();
				x.UseLinuxIfAvailable();
			});
		}
	}
}