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

namespace Deveel.Data.Sql.Constraints {
	public abstract class ConstraintInfo : IDbObjectInfo, ISqlFormattable {
		protected ConstraintInfo(ObjectName tableName, string constraintName) {
			TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
			ConstraintName = constraintName ?? throw new ArgumentNullException(nameof(constraintName));
			Deferrability = ConstraintDeferrability.InitiallyImmediate;
		}

		DbObjectType IDbObjectInfo.ObjectType => DbObjectType.Constraint;

		public string ConstraintName { get; }

		public ObjectName TableName { get; }

		public ConstraintDeferrability Deferrability { get; set; }

		public ObjectName FullName => new ObjectName(TableName, ConstraintName);

		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			AppendTo(builder);
		}

		protected virtual void AppendTo(SqlStringBuilder builder) {

		}
	}
}