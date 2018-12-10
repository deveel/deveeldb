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

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql {
	/// <summary>
	/// A single parameter value in a <see cref="SqlCommand"/>.
	/// </summary>
	/// <remarks>
	/// <para>
	/// Parameters carry a collateral value references by the command
	/// text of a <see cref="SqlCommand"/>, specifying the type of value.
	/// </para>
	/// <para>
	/// The system will deference parameters while executing a
	/// <see cref="SqlCommand"/> and substitute the references with
	/// the value and type carried by a parameter during the execution
	/// of the command.
	/// </para>
	/// </remarks>
	public sealed class SqlParameter {
		/// <summary>
		/// Constructs a parameter of the given type and with
		/// null value. 
		/// </summary>
		/// <param name="sqlType">The SQL type of the parameter</param>
		/// <remarks>
		/// This overload of the constructor will set the <see cref="Name"/>
		/// of the parameter to the <see cref="Marker"/>, implying
		/// a <see cref="SqlParameterNaming.Marker"/> naming convention
		/// of the command.
		/// </remarks>
		public SqlParameter(SqlType sqlType) 
			: this(sqlType, null) {
		}

		/// <summary>
		/// Constructs a parameter of the given type and with
		/// a  specified value 
		/// </summary>
		/// <param name="sqlType">The SQL type of the parameter</param>
		/// <param name="value">The value of the parameter</param>
		/// <remarks>
		/// This overload of the constructor will set the <see cref="Name"/>
		/// of the parameter to the <see cref="Marker"/>, implying
		/// a <see cref="SqlParameterNaming.Marker"/> naming convention
		/// of the command.
		/// </remarks>
		public SqlParameter(SqlType sqlType, ISqlValue value) 
			: this(Marker, sqlType, value) {
		}

		/// <summary>
		/// Constructs a named parameter of the given type
		/// and with a null value
		/// </summary>
		/// <param name="name">The name of the parameter. This can be
		/// either a <see cref="Marker"/> value or a variable name.</param>
		/// <param name="sqlType">The SQL type of the parameter</param>
		public SqlParameter(string name, SqlType sqlType) 
			: this(name, sqlType, null) {
		}

		/// <summary>
		/// Constructs a named parameter of the given type
		/// and with a provided value.
		/// </summary>
		/// <param name="name">The name of the parameter. This can be
		/// either a <see cref="Marker"/> value or a variable name.</param>
		/// <param name="sqlType">The SQL type of the parameter</param>
		/// <param name="value">The value of the parameter</param>
		/// <exception cref="ArgumentNullException">If the specified
		/// <paramref name="sqlType"/> is <c>null</c> or the <paramref name="name"/>
		/// is <c>null</c> or empty.</exception>
		/// <exception cref="ArgumentException">If the specified <paramref name="name"/>
		/// is one character long and the single character is not the <see cref="Marker"/>
		/// or if the single character is the <see cref="NamePrefix"/>.</exception>
		public SqlParameter(string name, SqlType sqlType, ISqlValue value) {
			if (sqlType == null)
				throw new ArgumentNullException(nameof(sqlType));

			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException(nameof(name));

			if (!String.Equals(name, Marker, StringComparison.Ordinal) &&
			    name[0] == NamePrefix) {
				name = name.Substring(1);

				if (String.IsNullOrEmpty(name))
					throw new ArgumentException("Cannot specify only the variable bind prefix as parameter.");
			}

			Name = name;
			SqlType = sqlType;
			Value = value;
			Direction = SqlParameterDirection.In;
		}

		public SqlParameter(string name, SqlObject value)
			: this(name, value.Type, value.Value) {
		}

		/// <summary>
		/// The optional prefix character of a named parameter
		/// </summary>
		public const char NamePrefix = ':';

		/// <summary>
		/// The name of a marker parameter
		/// </summary>
		public const string Marker = "?";

		/// <summary>
		/// Gets the name of the parameter
		/// </summary>
		public string Name { get; }

		/// <summary>
		/// Gets the SQL type of the parameter.
		/// </summary>
		public SqlType SqlType { get; }

		/// <summary>
		/// Gets or sets the direction of the parameter
		/// </summary>
		/// <remarks>
		/// By default the direction of a parameter is set
		/// to <see cref="SqlParameterDirection.In"/>.
		/// </remarks>
		/// <seealso cref="SqlParameterDirection"/>
		public SqlParameterDirection Direction { get; set; }

		/// <summary>
		/// Gets or sets the value of the parameter.
		/// </summary>
		public ISqlValue Value { get; set; }
	}
}