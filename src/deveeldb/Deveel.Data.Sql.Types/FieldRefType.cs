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

namespace Deveel.Data.Sql.Types {
	public sealed class FieldRefType : SqlType {
		public FieldRefType(ObjectName fieldName)
			: base(String.Format("{0}%TYPE", fieldName), SqlTypeCode.FieldRef) {
			if (fieldName == null)
				throw new ArgumentNullException("fieldName");

			FieldName = fieldName;
		}

		public ObjectName FieldName { get; private set; }

		public override bool IsReference {
			get { return true; }
		}

		public override SqlType Resolve(IRequest context) {
			return context.Access().ResolveFieldType(FieldName);
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			FieldName.AppendTo(builder);
			builder.Append("%TYPE");
		}
	}
}
