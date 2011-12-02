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

namespace Deveel.Data.Sql {
	/// <summary>
	/// Represents a single argument in the definition of a function.
	/// </summary>
	public sealed class FunctionArgument {
		/// <summary>
		/// Constructs the argument with the given name and type.
		/// </summary>
		/// <param name="name">The name of the argument.</param>
		/// <param name="type">The <see cref="TType"/> of the function
		/// argument.</param>
		/// <exception cref="ArgumentNullException">
		/// If either <paramref name="name"/> or <paramref name="type"/>
		/// are <c>null</c> or empty.
		/// </exception>
		public FunctionArgument(string name, TType type) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");
			if (type == null)
				throw new ArgumentNullException("type");

			this.name = name;
			this.type = type;
		}

		private readonly string name;
		private readonly TType type;

		/// <summary>
		/// Gets the name of the argument.
		/// </summary>
		public string Name {
			get { return name; }
		}

		/// <summary>
		/// Gets the <see cref="TType"/> of the argument.
		/// </summary>
		public TType Type {
			get { return type; }
		}

		/// <inheritdoc/>
		public override string ToString() {
			return name + " " + type.ToSQLString();
		}
	}
}