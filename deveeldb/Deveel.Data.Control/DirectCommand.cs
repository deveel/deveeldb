// 
//  Copyright 2010  Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

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