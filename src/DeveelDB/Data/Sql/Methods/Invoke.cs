// 
//  Copyright 2010-2018 Deveel
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
//

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace Deveel.Data.Sql.Methods {
	/// <summary>
	/// Represents the invoke of a method, either function or procedure, 
	/// defined by a system.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The natural form of an invocation of a method includes the name
	/// of the method to invoke and an optional list of parameters.
	/// </para>
	/// <para>
	/// Arguments of an invoke can be optionally named, making them
	/// to be passed in the position defined by the corresponding parameters
	/// with the same name. When arguments are not named, they are associated
	/// with the sequential order of the parameters of the method.
	/// </para>
	/// <para>
	/// An invoke cannot contain named and unnamed arguments.
	/// </para>
	/// <para>
	/// The name of the method must be unique within a schema context (eg.
	/// a function cannot have the same name of another function or another
	/// procedure within the same schema). When the name of the method specified
	/// is simple (no schema definition), this is resolved against the system 
	/// functions and procedures, and if not found it will be resolved against
	/// the whole set of methods present in the system: an error will occur if
	/// methods with the same simple name are present in multiple schema.
	/// </para>
	/// </remarks>
	public sealed class Invoke : ISqlFormattable {
		/// <summary>
		/// Constructs an instance of <see cref="Invoke"/> to the given method,
		///  without any arguments.
		/// </summary>
		/// <param name="methodName">The name of the method to invoke</param>
		public Invoke(ObjectName methodName) 
			: this(methodName, new InvokeArgument[0]) {
		}

		/// <summary>
		/// Constructs an instance of <see cref="Invoke"/> to the given method and
		/// a given list of arguments.
		/// </summary>
		/// <param name="methodName">The name of the method to invoke</param>
		/// <param name="args">The list of arguments of the invoke</param>
		/// <exception cref="ArgumentNullException">If the provided <paramref name="methodName"/>
		/// is <c>null</c> or empty.</exception>
		public Invoke(ObjectName methodName, InvokeArgument[] args) {
			if (ObjectName.IsNullOrEmpty(methodName))
				throw new ArgumentNullException(nameof(methodName));

			MethodName = methodName;
			Arguments = new ArgumentList(this);

			if (args != null) {
				foreach (var arg in args) {
					Arguments.Add(arg);
				}
			}
		}

		/// <summary>
		/// Gets the name of the method invoked.
		/// </summary>
		public ObjectName MethodName { get; }

		/// <summary>
		/// Gets a mutable list of arguments to the method invoked
		/// </summary>
		/// <remarks>
		/// If the invoke was constructed with any arguments passed,
		/// these will be already part of the list.
		/// </remarks>
		public IList<InvokeArgument> Arguments { get; }

		/// <summary>
		/// Gets a boolean value indicating if the invoke contains any
		/// named arguments.
		/// </summary>
		public bool IsNamed => Arguments.Any(x => x.IsNamed);

		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			MethodName.AppendTo(builder);
			builder.Append("(");

			if (Arguments.Count > 0) {
				for (int i = 0; i < Arguments.Count; i++) {
					Arguments[i].AppendTo(builder);

					if (i < Arguments.Count - 1)
						builder.Append(", ");
				}
			}

			builder.Append(")");
		}

		public override string ToString() {
			return this.ToSqlString();
		}

		#region ArgumentList

		class ArgumentList : Collection<InvokeArgument> {
			private readonly Invoke invoke;

			public ArgumentList(Invoke invoke) {
				this.invoke = invoke;
			}

			private void ValidateArgument(InvokeArgument item) {
				if (!item.IsNamed && invoke.IsNamed)
					throw new ArgumentException("The invoke context has named items");
				if (item.IsNamed && !invoke.IsNamed && Items.Count > 0)
					throw new ArgumentException("Cannot insert a named item in an anonymous context");

				if (invoke.IsNamed) {
					foreach (var argument in Items) {
						// TODO: case insensitive comparison here?
						if (String.Equals(argument.Name, item.Name, StringComparison.Ordinal))
							throw new ArgumentException($"An argument with name '{item.Name}' is already defined in the list.");
					}
				}
			}

			protected override void SetItem(int index, InvokeArgument item) {
				ValidateArgument(item);

				if (item.IsNamed) {
					var existing = base.Items[index];
					if (existing.Name != item.Name)
						throw new ArgumentException($"The argument at index {index} has a different name than '{item.Name}'");
				}

				base.SetItem(index, item);
			}

			protected override void InsertItem(int index, InvokeArgument item) {
				ValidateArgument(item);
				base.InsertItem(index, item);
			}
		}

		#endregion
	}
}