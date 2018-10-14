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

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Methods {
	/// <summary>
	/// Defines the properties of a single argument of an invocation to
	/// a method in a system.
	/// </summary>
	/// <seealso cref="Invoke"/>
	public sealed class InvokeArgument : ISqlFormattable {
		/// <summary>
		/// Constructs an instance of <see cref="InvokeArgument"/> that is unnamed
		/// and defines a given value for the argument
		/// </summary>
		/// <param name="value">The value of the argument</param>
		/// <exception cref="ArgumentNullException">If the given <paramref name="value"/>
		/// is <c>null</c></exception>
		public InvokeArgument(SqlExpression value) 
			: this(null, value) {
		}

		/// <summary>
		/// Constructs an instance of a named <see cref="InvokeArgument"/>
		/// and defines a given value for the argument
		/// </summary>
		/// <param name="name">The name of the argument, to associate
		/// the value of the argument to the parameter of the method invoked. If 
		/// <c>null</c> is passed this argument will be considered unnamed.</param>
		/// <param name="value">The value of the argument</param>
		/// <exception cref="ArgumentNullException">If the given <paramref name="value"/>
		/// is <c>null</c></exception>
		public InvokeArgument(string name, SqlExpression value) {
			if (value == null)
				throw new ArgumentNullException(nameof(value));

			Name = name;
			Value = value;
		}

		/// <summary>
		/// Constructs an instance of <see cref="InvokeArgument"/> that is unnamed
		/// and defines a constant value for the argument.
		/// </summary>
		/// <param name="value">The value of the argument</param>
		/// <remarks>
		/// This method is a shortcut to the creation of a <see cref="SqlConstantExpression"/>
		/// to pass as argument of <see cref="InvokeArgument(SqlExpression)"/>.
		/// </remarks>
		public InvokeArgument(SqlObject value)
			: this(null, value) {
		}

		/// <summary>
		/// Constructs a named instance of <see cref="InvokeArgument"/> and
		/// defines a constant value for the argument.
		/// </summary>
		/// <param name="name">The name of the argument, to associate
		/// the value of the argument to the parameter of the method invoked. If 
		/// <c>null</c> is passed this argument will be considered unnamed.</param>
		/// <param name="value">The value of the argument</param>
		/// <remarks>
		/// This method is a shortcut to the creation of a <see cref="SqlConstantExpression"/>
		/// to pass as argument of <see cref="InvokeArgument(string, SqlExpression)"/>.
		/// </remarks>
		public InvokeArgument(string name, SqlObject value)
			: this(name, SqlExpression.Constant(value)) {
		}

		/// <summary>
		/// Gets the name of the argument, if any is specified.
		/// </summary>
		/// <remarks>
		/// Named arguments are associated with parameters of the
		/// method invoked that have the same name.
		/// </remarks>
		public string Name { get; }

		/// <summary>
		/// Gets a boolean value indicating whether this argument is named or not.
		/// </summary>
		public bool IsNamed => !String.IsNullOrEmpty(Name);

		/// <summary>
		/// Gets the value of the argument to be passed to the method.
		/// </summary>
		public SqlExpression Value { get; }

		public override string ToString() {
			return this.ToSqlString();
		}

		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			if (IsNamed)
				builder.AppendFormat("{0} => ", Name);

			Value.AppendTo(builder);
		}
	}
}