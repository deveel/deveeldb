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
using System.Runtime.Serialization;
using System.Text;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Routines {
	[Serializable]
	public sealed class InvokeArgument : ISerializable, IPreparable {
		public InvokeArgument(SqlExpression value) 
			: this(null, value) {
		}

		public InvokeArgument(string name, SqlExpression value) {
			if (value == null)
				throw new ArgumentNullException("value");

			Name = name;
			Value = value;
		}

		private InvokeArgument(SerializationInfo info, StreamingContext context) {
			Name = info.GetString("Name");
			Value = (SqlExpression) info.GetValue("Value", typeof(SqlExpression));
		}

		public string Name { get; private set; }

		public SqlExpression Value { get; private set; }

		public bool IsNamed {
			get { return !String.IsNullOrEmpty(Name); }
		}

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Name", Name);
			info.AddValue("Value", Value);
		}

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			var preparedValue = Value.Prepare(preparer);
			return new InvokeArgument(Name, preparedValue);
		}

		public override string ToString() {
			var sb = new StringBuilder();
			if (IsNamed) {
				sb.AppendFormat("{0} => ", Name);
			}

			sb.Append(Value);

			return sb.ToString();
		}
	}
}
