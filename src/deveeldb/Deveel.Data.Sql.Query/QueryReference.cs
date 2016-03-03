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

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Query {
	public sealed class QueryReference {
		public QueryReference(ObjectName name, int level) {
			Level = level;
			Name = name;
		}

		public int Level { get; private set; }

		public ObjectName Name { get; private set; }

		public Field Value { get; private set; }

		public SqlType ReturnType {
			get { return Value == null ? null : Value.Type; }
		}

		public Field Evaluate(IVariableResolver resolver) {
			Value = resolver.Resolve(Name);
			return Value;
		}
	}
}