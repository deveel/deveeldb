// 
//  Copyright 2010-2015 Deveel
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

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql.Statements {
	/// <summary>
	/// An objects that is the result of a preparation of a parent
	/// <see cref="SqlStatement"/>, that is ready to be executed.
	/// </summary>
	/// <remarks>
	/// This object provides the final model of a SQL statement
	/// that is prepared and can be stored and retrieved for
	/// later execution.
	/// </remarks>
	public abstract class SqlPreparedStatement : SqlStatement {
		protected SqlPreparedStatement(SqlStatement source) {
			if (source == null)
				throw new ArgumentNullException("source");

			Source = source;
		}

		/// <summary>
		/// Gets the statement that is the source of this statement.
		/// </summary>
		public SqlStatement Source { get; private set; }

		protected override bool IsPreparable {
			get { return false; }
		}
	}
}
