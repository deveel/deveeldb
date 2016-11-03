// 
//  Copyright 2010-2016 Deveel
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
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Statements.Build {
	public interface IColumnBuilder {
		IColumnBuilder Named(string name);

		IColumnBuilder OfType(SqlType type);

		IColumnBuilder NotNull(bool value = true);

		IColumnBuilder Identity(bool value = true);

		IColumnBuilder WithIndexType(string value);

		IColumnBuilder WithDefault(SqlExpression expression);

		IColumnBuilder WithConstraint(ColumnConstraintInfo constraintInfo);
	}
}
