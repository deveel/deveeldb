//  
//  DirectCommand.cs
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
using System.Collections;

namespace Deveel.Data.Control {
	public sealed class DirectCommand : ICloneable {
		public DirectCommand(DirectCommandType commandType, IDictionary args) {
			this.commandType = commandType;
			this.args = new Hashtable(args);
		}

		public DirectCommand(DirectCommandType commandType)
			: this(commandType, (IDictionary) null) {
		}

		internal DirectCommand(DirectCommandType commandType, params object[] args)
			: this(commandType, FormArguments(args)) {
		}

		private readonly DirectCommandType commandType;
		private readonly Hashtable args;

		public DirectCommandType CommandType {
			get { return commandType; }
		}

		public object this[string name] {
			get { return GetArgument(name); }
			set { SetArgument(name, value); }
		}

		public int ArgumentCount {
			get { return args.Count; }
		}

		public object GetArgument(string name) {
			return args[name];
		}

		internal object GetOptional(string name, object defaultValue) {
			object value = GetArgument(name);
			return (value == null ? defaultValue : value);
		}

		internal object GetRequired(string name) {
			object value = GetArgument(name);
			if (value == null)
				throw new ArgumentException("The required argument '" + name + "' was not set.");

			return value;
		}

		public void SetArgument(string name, object value) {
			args[name] = value;
		}

		internal object[] ToArgsArray() {
			object[] array = new object[args.Count];
			args.Values.CopyTo(array, 0);
			return array;
		}

		#region Implementation of ICloneable

		public object Clone() {
			return new DirectCommand(commandType, (IDictionary) args.Clone());
		}

		#endregion

		private static IDictionary FormArguments(object[] args) {
			if (args == null || args.Length == 0)
				return null;

			if (args.Length % 2 != 0)
				throw new ArgumentException();

			Hashtable dict = new Hashtable(args.Length / 2);

			for (int i = 0; i < args.Length; i += 2) {
				object argName = args[i];
				object argValue = args[i + 1];

				if (!typeof(string).IsInstanceOfType(argName))
					throw new ArgumentException();

				dict[argName] = argValue;
			}

			return dict;
		}
	}
}