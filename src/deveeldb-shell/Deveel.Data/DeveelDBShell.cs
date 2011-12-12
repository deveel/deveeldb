//
//  Copyright 2011 Deveel
//
//  This file is part of DeveelDBShell.
//
//  DeveelDBShell is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  DeveelDBShell is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
//
//  You should have received a copy of the GNU General Public License
//  along with DeveelDBShell. If not, see <http://www.gnu.org/licenses/>.
//

using System;
using System.Reflection;

using Deveel.Configuration;
using Deveel.Console;
using Deveel.Console.Commands;
using Deveel.Data.Commands;

namespace Deveel.Data {
	public sealed class DeveelDBShell : ShellApplication, IInterruptable {
		protected override void RegisterCommands() {
			Commands.Register(typeof (ConnectCommand));
		}

		protected override Options CreateOptions() {
			Options options = new Options();
			options.AddOption("u", "user", true, "The name of the user that connects to the database.");
			options.AddOption("p", "password", true, "The password used to identify the user.");
			options.AddOption("d", "database", true, "The name of the database to connect to.");
			options.AddOption("?", "help", false, "Print the help message");
			return options;
		}

		protected override void OnExit() {
			base.OnExit();
		}

		private Version GetVersion() {
			Assembly assembly = typeof (DeveelDBShell).Assembly;
			return assembly.GetName().Version;
		}

		protected override void OnRunning() {
			Version version = GetVersion();

			Out.WriteLine("-----------------------------------------------------------------------------");
			Out.WriteLine(" Deveel DB Shell {0} Copyright (C) 2011 Deveel", version.ToString(3));
			Out.WriteLine();
			Out.WriteLine(" Deveel DB Shell is provided AS IS and comes with ABSOLUTELY NO WARRANTY");
			Out.WriteLine(" This is free software, and you are welcome to redistribute it under the");
			Out.WriteLine(" conditions of the GNU Public License <http://www.gnu.org/licenses/gpl.txt>");
			Out.WriteLine("----------------------------------------------------------------------------");
			Out.WriteLine();
			Out.WriteLine();
		}

		protected override ICommandSeparator CreateSeparator() {
			return new SqlCommandSeparator();
		}

		[STAThread]
		static void Main(string[] args) {
			DeveelDBShell shell = new DeveelDBShell();
			shell.SetPrompt("deveeldb> ");
			shell.Interrupted += ShellInterrupted;

			shell.HandleCommandLine(args);

			try {
				shell.Run(args);
			} catch (Exception) {
				shell.Shutdown();
			}
		}

		static void ShellInterrupted(object sender, EventArgs e) {
			ShellApplication shell = (ShellApplication) sender;
			shell.Exit(3);
		}
	}
}