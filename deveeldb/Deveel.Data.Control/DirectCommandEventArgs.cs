//  
//  DirectCommandEventArgs.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
// 
//  Copyright (c) 2009 Deveel
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


using System;
using System.Runtime.Serialization;

namespace Deveel.Data.Control {
	public sealed class DirectCommandEventArgs : EventArgs {
		internal DirectCommandEventArgs(DirectCommandType commandType, object[] args)
			: this(commandType, args, null) {
		}

		internal DirectCommandEventArgs(DirectCommandType commandType, object[] args, Exception error)
			: this(commandType, args, (object) null) {
			this.error = error;
		}

		internal DirectCommandEventArgs(DirectCommandType commandType, object[] args, object result) {
			this.commandType = commandType;
			this.args = args;
			this.result = result;
		}

		private readonly DirectCommandType commandType;
		private readonly object[] args;
		private readonly object result;
		private readonly Exception error;

		public Exception Error {
			get { return error; }
		}

		public object Result {
			get { return result; }
		}

		public object[] Arguments {
			get { return args; }
		}

		public DirectCommandType CommandType {
			get { return commandType; }
		}
	}
}