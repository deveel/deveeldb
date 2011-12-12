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

using Deveel.Console;
using Deveel.Console.Commands;

namespace Deveel.Data.Commands {
	public sealed class ExitCommand : Command {
		public override CommandResultCode Execute(IExecutionContext context, CommandArguments args) {
			Application.Exit(0);
			return CommandResultCode.Success;
		}

		public override string Name {
			get { return "exit"; }
		}
	}
}